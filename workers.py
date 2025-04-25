import os
import re
import copy
import json
import requests
import logging
from datetime import datetime
from socket import getfqdn
from time import sleep

from concurrent.futures import ThreadPoolExecutor

from PyQt5 import QtCore, QtWidgets, QtGui


class CreateEvent(QtCore.QThread):
    """
    Background worker thread to capture and process network device data
    and save it as a snapshot. Emits a signal when the snapshot is created.
    """
    add_snapshot_signal = QtCore.pyqtSignal(dict)

    def __init__(self, form):
        """
        Initialize the worker with form data.

        Args:
            form (QWidget): Reference to the parent form containing UI fields.
        """
        super().__init__()
        self.form = form

    def run(self):
        """
        Main method executed when the thread starts. Collects, processes,
        and saves network snapshot data.
        """
        os.makedirs(self.form.output_dir, exist_ok=True)

        self.name = self.form.name_line_edit.text()
        self.type = self.form.type_combobox.currentText()
        self.devices = list(filter(None, self.form.device_text_edit.toPlainText().splitlines()))
        logging.info('Snapshot worker started.')

        self.data = {'endpoints': {}}
        self.load_mac_vendor()
        self.thread_executor()
        self.refactor_data()
        self.save_snapshot()

        logging.info('Snapshot worker finished.')

    def thread_executor(self):
        """
        Executes device data collection in parallel using threads.
        """
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = {}
            for device in self.devices:
                futures[device] = executor.submit(self.create_task, device)
                sleep(0.5)

            for device, future in futures.items():
                exception = future.exception()
                if exception:
                    logging.error(f'Exception for {device}: {exception}')

    def create_task(self, device):
        """
        Connects to the device and collects endpoint data.

        Args:
            device (str): IP or hostname of the network device.
        """
        from netcore import GenericHandler

        logging.info(f'Connecting to {device}...')
        proxy = {
            'hostname': self.form.session['JUMPHOST_IP'],
            'username': self.form.session['JUMPHOST_USERNAME'],
            'password': self.form.session['JUMPHOST_PASSWORD']
        } if self.form.session['JUMPHOST_IP'] else None

        try:
            handler = GenericHandler(
                hostname=device,
                username=self.form.session['NETWORK_USERNAME'],
                password=self.form.session['NETWORK_PASSWORD'],
                proxy=proxy,
                handler='NETMIKO'
            )
            logging.info(f'Connection established to {device}')
        except Exception:
            logging.error(f'Connection failed to {device}')
            return

        logging.info(f'Capturing and parsing data for {device}')
        mac_data = handler.sendCommand(cmd='show mac address', autoParse=True, key='mac_address')
        arp_data = handler.sendCommand(cmd='show ip arp', autoParse=True, key='mac_address')
        iface_data = handler.sendCommand(cmd='show interface status', autoParse=True, key='interface')

        logging.info(f'Processing endpoint data for {device}')
        endpoint_data = {}
        idx = 0

        for mac, mac_prop in mac_data.items():
            if not re.search(r'^(Te|Gi|Fa|Eth|Two|Twe)', mac_prop['ports']):
                continue

            idx += 1
            hostname = getfqdn(arp_data[mac]['ip_address']) if arp_data.get(mac) else 'Unknown'
            ip_address = arp_data[mac]['ip_address'] if arp_data.get(mac) else 'Unknown'
            vendor = self.get_mac_vendor(mac)

            endpoint_data[mac] = {
                'Hostname': hostname,
                'IP Address': ip_address,
                'Switch': handler.base_prompt,
                'Interface': mac_prop['ports'],
                'Speed': '',
                'Duplex': '',
                'MAC Address': mac,
                'Vendor': vendor,
                'Vlan': mac_prop['vlan_id']
            }

            for iface, iface_prop in iface_data.items():
                if self.normalize_iface(iface) == self.normalize_iface(mac_prop['ports']):
                    endpoint_data[mac]['Speed'] = iface_prop['speed']
                    endpoint_data[mac]['Duplex'] = iface_prop['duplex']

        self.data['endpoints'][device] = endpoint_data

    def normalize_iface(self, iface):
        """
        Normalizes interface names to a consistent format.

        Args:
            iface (str): Interface name.

        Returns:
            str: Normalized interface name.
        """
        labels = ['Te', 'Gi', 'Fa', 'Eth', 'Lo', 'Vl', 'Two', 'Twe']
        for label in labels:
            if re.search(f'^{label}', iface, re.IGNORECASE):
                port_id = re.search(r'(\d+\S*)', iface).group(0)
                return f'{label}{port_id}'
        return iface

    def refactor_data(self):
        """
        Refactors collected data to consolidate duplicate MAC entries across switches.
        """
        logging.info('Refactoring snapshot data...')
        ep_idx = 0
        refactored = {}
        mac_to_index = {}

        for device in self.devices:
            for mac, prop in self.data['endpoints'].get(device, {}).items():
                if mac in mac_to_index:
                    idx = mac_to_index[mac]
                    refactored[idx]['Switch'].append(prop['Switch'])
                    refactored[idx]['Interface'].append(prop['Interface'])
                    refactored[idx]['Speed'].append(prop['Speed'])
                    refactored[idx]['Duplex'].append(prop['Duplex'])
                else:
                    ep_idx += 1
                    mac_to_index[mac] = ep_idx
                    refactored[ep_idx] = {
                        'MAC Address': prop['MAC Address'],
                        'Vendor': prop['Vendor'],
                        'Hostname': prop['Hostname'],
                        'IP Address': prop['IP Address'],
                        'Vlan': prop['Vlan'],
                        'Switch': [prop['Switch']],
                        'Interface': [prop['Interface']],
                        'Speed': [prop['Speed']],
                        'Duplex': [prop['Duplex']]
                    }

        self.data['endpoints'] = refactored

    def save_snapshot(self):
        """
        Saves the processed data into a timestamped JSON file and emits signal.
        """
        logging.info('Saving snapshot to disk...')
        snapshots_path = os.path.join(self.form.output_dir, 'Snapshots')
        if not os.path.exists(snapshots_path):
            os.mkdir(snapshots_path)

        timestamp = datetime.now().strftime('%Y-%m-%d_%H.%M')
        filename = os.path.join(snapshots_path, f"[{self.type}]_[{self.name}]_[{timestamp}].json")

        json.dump(self.data, open(filename, 'w'), indent=4)
        self.add_snapshot_signal.emit({
            'timestamp': timestamp,
            'type': self.type,
            'name': self.name
        })

    def load_mac_vendor(self):
        """
        Loads MAC vendor OUI data from cache or fetches latest from IEEE if outdated.
        """
        vendor_file = os.path.join(os.path.dirname(__file__), 'macvendor_registry.json')
        if not os.path.exists(vendor_file) or (
                datetime.now() - datetime.fromtimestamp(os.path.getmtime(__file__))
        ).days > 90:
            logging.info('Fetching latest OUI data from IEEE...')
            oui_text = requests.get('https://standards-oui.ieee.org/oui/oui.txt').text
            oui_data = {}

            for line in oui_text.splitlines():
                match = re.search(r'(\w+)\s+\(base 16\)\s+(.*)', line)
                if match:
                    oui_data[match.group(1)] = match.group(2)

            json.dump(oui_data, open(vendor_file, 'w'), indent=4)

        self.oui_data = json.load(open(vendor_file))

    def get_mac_vendor(self, mac):
        """
        Returns the vendor name associated with a MAC address.

        Args:
            mac (str): MAC address.

        Returns:
            str: Vendor name.
        """
        normalized = re.sub(r'[.\-:]', '', mac).upper()[:6]
        return self.oui_data.get(normalized, 'Unknown')


class CompareEvent(QtCore.QThread):
    """
    Worker thread that performs snapshot comparison in the background.

    This class compares MAC address endpoint data between a pre and post snapshot
    to determine changes in the network state, and formats results accordingly.
    """

    def __init__(self, form):
        """
        Initialize the worker thread with form context.

        Args:
            form (QWidget): The form providing snapshot selection and output directory.
        """
        super().__init__()
        self.form = form

    def run(self):
        """
        Entry point for the worker thread.

        Loads pre and post snapshot data, processes MAC-based comparisons,
        and prepares a unified comparison result with formatted outputs.
        """
        logging.debug("WorkerCompareEvent started.")
        snapshots = self.form.get_selected_items()
        snapshots_path = os.path.join(self.form.output_dir, 'Snapshots')

        # Determine which is pre and post snapshot
        pre_snapshot = snapshots[0] if snapshots[0]['type'] == 'Pre' else snapshots[1]
        post_snapshot = snapshots[0] if snapshots[0]['type'] == 'Post' else snapshots[1]

        # Load JSON snapshot data
        pre_path = os.path.join(snapshots_path,
                                f"[{pre_snapshot['type']}]_[{pre_snapshot['name']}]_[{pre_snapshot['timestamp']}].json")
        post_path = os.path.join(snapshots_path,
                                 f"[{post_snapshot['type']}]_[{post_snapshot['name']}]_[{post_snapshot['timestamp']}].json")

        with open(pre_path) as pre_file, open(post_path) as post_file:
            self.pre_snapshot_data = json.load(pre_file)['endpoints']
            self.post_snapshot_data = json.load(post_file)['endpoints']

        logging.info("Loaded pre and post snapshot data.")

        self.pre_snapshot_data, self.post_snapshot_data = self.get_mac_data()
        self.compare_snapshots()
        self.write_report()

    def get_mac_data(self):
        """
        Reformats snapshot data by using MAC address as key.

        Returns:
            tuple: Reformatted pre and post snapshot dictionaries.
        """
        logging.debug("Reformatting snapshot data by MAC address.")

        pre_data = {
            ep['MAC Address']: ep for ep in self.pre_snapshot_data.values()
        }
        post_data = {
            ep['MAC Address']: ep for ep in self.post_snapshot_data.values()
        }
        return pre_data, post_data

    def compare_snapshots(self):
        """
        Compares pre and post snapshot data to detect MAC address changes.

        Builds a dictionary of comparison results with appropriate formatting for UI display.
        """
        logging.debug("Starting snapshot comparison.")
        self.cell_format = {
            'ftNormal': {'font_color': '#000000'},
            'ftBad': {'bg_color': '#FFC7CE', 'font_color': '#9C0006'},
            'ftGood': {'bg_color': '#C6EFCE', 'font_color': '#006100'},
            'ftInfo': {'bg_color': '#FFEB9C', 'font_color': '#9C6500'},
        }
        self.compare_data = {}

        # Process MACs from pre-snapshot
        for mac, pre in self.pre_snapshot_data.items():
            post = self.post_snapshot_data.get(mac)
            observation = 'MAC Not Learnt'
            obs_format = self.cell_format['ftBad']

            if post:
                observation = 'MAC Learnt'
                obs_format = self.cell_format['ftGood']

            self.compare_data[mac] = {
                'Address': {'value': mac, 'cellFormat': self.cell_format['ftNormal']},
                'Observation': {'value': observation, 'cellFormat': obs_format},
                'Vendor': {'value': pre['Vendor'], 'cellFormat': self.cell_format['ftNormal']},
                'Pre-Device': {'value': pre['Switch'], 'cellFormat': self.cell_format['ftNormal']},
                'Pre-Interface': {'value': pre['Interface'], 'cellFormat': self.cell_format['ftNormal']},
                'Pre-Speed': {'value': pre['Speed'], 'cellFormat': self.cell_format['ftNormal']},
                'Pre-Duplex': {'value': pre['Duplex'], 'cellFormat': self.cell_format['ftNormal']},
                'Pre-Vlan': {'value': pre['Vlan'], 'cellFormat': self.cell_format['ftNormal']},
                'Pre-IP': {'value': pre['IP Address'], 'cellFormat': self.cell_format['ftNormal']},
                'Pre-Hostname': {'value': pre['Hostname'], 'cellFormat': self.cell_format['ftNormal']},
                'Post-Device': {'value': '???', 'cellFormat': self.cell_format['ftBad']},
                'Post-Interface': {'value': '???', 'cellFormat': self.cell_format['ftBad']},
                'Post-Speed': {'value': '???', 'cellFormat': self.cell_format['ftBad']},
                'Post-Duplex': {'value': '???', 'cellFormat': self.cell_format['ftBad']},
                'Post-Vlan': {'value': '???', 'cellFormat': self.cell_format['ftBad']},
                'Post-IP': {'value': '???', 'cellFormat': self.cell_format['ftBad']},
                'Post-Hostname': {'value': '???', 'cellFormat': self.cell_format['ftBad']}
            }

            if post:
                self.compare_data[mac].update({
                    'Post-Device': {'value': post['Switch'], 'cellFormat': self.cell_format['ftNormal']},
                    'Post-Interface': {'value': post['Interface'], 'cellFormat': self.cell_format['ftNormal']},
                    'Post-Speed': self.match_attribute(pre['Speed'], post['Speed']),
                    'Post-Duplex': self.match_attribute(pre['Duplex'], post['Duplex']),
                    'Post-Vlan': self.match_attribute(pre['Vlan'], post['Vlan']),
                    'Post-IP': self.match_attribute(pre['IP Address'], post['IP Address']),
                    'Post-Hostname': self.match_attribute(pre['Hostname'], post['Hostname']),
                })

            if hasattr(logging, 'savings'):
                logging.savings(1)

        # Process new MACs from post-snapshot
        for mac, post in self.post_snapshot_data.items():
            if mac not in self.pre_snapshot_data:
                self.compare_data[mac] = {
                    'Address': {'value': mac, 'cellFormat': self.cell_format['ftNormal']},
                    'Observation': {'value': 'New MAC', 'cellFormat': self.cell_format['ftInfo']},
                    'Vendor': {'value': post['Vendor'], 'cellFormat': self.cell_format['ftNormal']},
                    'Pre-Device': {'value': '???', 'cellFormat': self.cell_format['ftNormal']},
                    'Pre-Interface': {'value': '???', 'cellFormat': self.cell_format['ftNormal']},
                    'Pre-Speed': {'value': '???', 'cellFormat': self.cell_format['ftNormal']},
                    'Pre-Duplex': {'value': '???', 'cellFormat': self.cell_format['ftNormal']},
                    'Pre-Vlan': {'value': '???', 'cellFormat': self.cell_format['ftNormal']},
                    'Pre-IP': {'value': '???', 'cellFormat': self.cell_format['ftNormal']},
                    'Pre-Hostname': {'value': '???', 'cellFormat': self.cell_format['ftNormal']},
                    'Post-Device': {'value': post['Switch'], 'cellFormat': self.cell_format['ftNormal']},
                    'Post-Interface': {'value': post['Interface'], 'cellFormat': self.cell_format['ftNormal']},
                    'Post-Speed': {'value': post['Speed'], 'cellFormat': self.cell_format['ftNormal']},
                    'Post-Duplex': {'value': post['Duplex'], 'cellFormat': self.cell_format['ftNormal']},
                    'Post-Vlan': {'value': post['Vlan'], 'cellFormat': self.cell_format['ftNormal']},
                    'Post-IP': {'value': post['IP Address'], 'cellFormat': self.cell_format['ftNormal']},
                    'Post-Hostname': {'value': post['Hostname'], 'cellFormat': self.cell_format['ftNormal']}
                }

                if hasattr(logging, 'savings'):
                    logging.savings(1)

        logging.info("Snapshot comparison completed.")

    def match_attribute(self, pre, post):
        """
        Compare a single attribute and apply conditional formatting.

        Args:
            pre (str): Pre-snapshot value.
            post (str): Post-snapshot value.

        Returns:
            dict: Dictionary with value and formatting based on match.
        """
        cell_style = self.cell_format['ftNormal'] if pre == post else self.cell_format['ftBad']
        return {'value': post, 'cellFormat': cell_style}

    def write_report(self):
        """
        Writes the MAC comparison results to an Excel file with formatted headers and data.
        """
        from netcore import XLBW

        # Set the report output path
        timestamp = datetime.now().strftime('%Y-%m-%d_%H.%M')
        filename = f"{os.path.basename(os.path.dirname(__file__)).title()}_{timestamp}.xlsx"
        self.form.output_report = os.path.join(self.form.output_dir, filename)

        workbook = XLBW(self.form.output_report)
        worksheet = workbook.add_worksheet('Mac Compare')

        base_fmt = {'font_size': '10', 'font_name': 'Segoe UI', 'font_color': '#000000', 'valign': 'top'}
        header1_fmt = {'font_size': '10', 'font_name': 'Segoe UI', 'align': "center", 'font_color': '#FFFFFF',
                       'valign': 'top', 'bg_color': "#1F4E78"}
        header2_fmt = {'font_size': '10', 'font_name': 'Segoe UI', 'align': "center", 'font_color': '#000000',
                       'valign': 'top', 'bg_color': "#E7E6E6"}
        pre_fmt = {'font_size': '10', 'font_name': 'Segoe UI', 'align': "center", 'font_color': '#FFFFFF',
                   'valign': 'top', 'bg_color': "#548235"}
        post_fmt = {'font_size': '10', 'font_name': 'Segoe UI', 'align': "center", 'font_color': '#FFFFFF',
                    'valign': 'top', 'bg_color': "#BF8F00"}
        pattern_fmt = {'pattern': 4}

        pre_col_idx, post_col_idx = 4, 12
        row_idx = 1
        data = {idx + 1: val for idx, (key, val) in enumerate(self.compare_data.items())}

        worksheet.write(row_idx, 0, '#', workbook.add_format(header1_fmt))
        col_idx = 1
        for key in data[1].items():
            if col_idx in (pre_col_idx, post_col_idx):
                col_idx += 1
            worksheet.write(row_idx, col_idx, key[0], workbook.add_format(header1_fmt))
            col_idx += 1

        row_idx += 1
        for row_key, row_values in data.items():
            col_idx = 0
            worksheet.write(row_idx, col_idx, row_key, workbook.add_format(base_fmt))
            col_idx += 1
            for _, cell in row_values.items():
                if col_idx in (pre_col_idx, post_col_idx):
                    col_idx += 1
                fmt = copy.deepcopy(base_fmt)
                if cell.get('cellFormat'):
                    fmt.update(cell['cellFormat'])
                cell_fmt = workbook.add_format(fmt)
                value = '\n'.join(cell['value']) if isinstance(cell['value'], list) else cell['value']
                worksheet.write(row_idx, col_idx, value, cell_fmt)
                if cell.get('comment'):
                    comment = '\n'.join(cell['comment']) if isinstance(cell['comment'], list) else cell['comment']
                    worksheet.write_comment(row_idx, col_idx, comment)
                col_idx += 1
            row_idx += 1

        worksheet.autofilter(1, 1, row_idx, col_idx - 1)

        headers = ['#', 'Address', 'Observation', 'Vendor']
        header_fmt = workbook.add_format(header1_fmt)
        col_idx = 0
        for h in headers:
            worksheet.write(0, col_idx, h, header_fmt)
            col_idx += 1
        col_idx += 1
        worksheet.merge_range(0, col_idx, 0, col_idx + 6, 'Pre', workbook.add_format(pre_fmt))
        col_idx += 8
        worksheet.merge_range(0, col_idx, 0, col_idx + 6, 'Post', workbook.add_format(post_fmt))

        sub_headers = [' ', ' ', ' ', ' ', None, 'Device', 'Interface', 'Speed', 'Duplex', 'Vlan', 'IP', 'Hostname',
                       None, 'Device', 'Interface', 'Speed', 'Duplex', 'Vlan', 'IP', 'Hostname']
        sub_fmt = workbook.add_format(header2_fmt)
        for idx, sub in enumerate(sub_headers):
            if sub:
                worksheet.write(1, idx, sub, sub_fmt)

        worksheet.set_column(pre_col_idx, pre_col_idx, 0.4, workbook.add_format(pattern_fmt))
        worksheet.set_column(post_col_idx, post_col_idx, 0.4, workbook.add_format(pattern_fmt))
        workbook.close()

        logging.info(f"Saved MAC comparison report: {self.form.output_report}")
