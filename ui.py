import logging
import os
import re
import json
from tempfile import mkstemp
from PyQt5 import QtWidgets, QtGui, QtCore
from .workers import CreateEvent, CompareEvent

from PyQt5 import QtWidgets, QtCore


class Ui_Form(object):
    """
    A PyQt5 UI form class for configuring and executing network diagnostics.

    """

    def setup_ui(self, form):
        """
        Set up the layout and UI elements of the diagnostics form.

        Args:
            form (QWidget): The parent widget to apply the layout and components to.
        """
        self.form = form
        self.layout = QtWidgets.QHBoxLayout(form)

        # --- Create Group Box ---
        self.create_group_box = QtWidgets.QGroupBox(form)
        self.create_group_box.setTitle("Create")
        self.layout.addWidget(self.create_group_box)
        self.create_layout = QtWidgets.QVBoxLayout(self.create_group_box)

        # --- Top Form Fields ---
        self.create_top_layout = QtWidgets.QHBoxLayout()
        self.create_top_layout.setSpacing(30)
        self.create_layout.addLayout(self.create_top_layout)

        # --- Name input ---
        self.name_layout = QtWidgets.QHBoxLayout()
        self.name_layout.setSpacing(10)
        self.name_label = QtWidgets.QLabel("Name", self.create_group_box)
        self.name_layout.addWidget(self.name_label)
        self.name_line_edit = QtWidgets.QLineEdit(self.create_group_box)
        self.name_line_edit.setMinimumSize(QtCore.QSize(0, 25))
        self.name_layout.addWidget(self.name_line_edit)
        self.create_top_layout.addLayout(self.name_layout)

        # --- Type combobox ---
        self.type_layout = QtWidgets.QHBoxLayout()
        self.type_layout.setSpacing(10)
        self.type_label = QtWidgets.QLabel("Type", self.create_group_box)
        self.type_layout.addWidget(self.type_label)
        self.type_combobox = QtWidgets.QComboBox(self.create_group_box)
        self.type_combobox.addItems(["Pre", "Post"])
        self.type_combobox.setMinimumSize(QtCore.QSize(80, 25))
        self.type_layout.addWidget(self.type_combobox)
        self.create_top_layout.addLayout(self.type_layout)

        # --- Devices text area ---
        self.device_group_box = QtWidgets.QGroupBox("Devices", self.create_group_box)
        self.device_layout = QtWidgets.QHBoxLayout(self.device_group_box)
        self.device_text_edit = QtWidgets.QPlainTextEdit(self.device_group_box)
        self.device_layout.addWidget(self.device_text_edit)
        self.create_layout.addWidget(self.device_group_box)

        # --- Create button ---
        self.create_button_layout = QtWidgets.QHBoxLayout()
        self.create_button_layout.addItem(QtWidgets.QSpacerItem(0, 0, QtWidgets.QSizePolicy.Expanding))
        self.create_button = QtWidgets.QPushButton("Create", self.create_group_box)
        self.create_button.setMinimumSize(QtCore.QSize(100, 30))
        self.create_button_layout.addWidget(self.create_button)
        self.create_button_layout.addItem(QtWidgets.QSpacerItem(0, 0, QtWidgets.QSizePolicy.Expanding))
        self.create_layout.addLayout(self.create_button_layout)

        # --- Snapshots Group Box ---
        self.snapshots_group_box = QtWidgets.QGroupBox("Snapshots", form)
        self.layout.addWidget(self.snapshots_group_box)
        self.snapshots_layout = QtWidgets.QVBoxLayout(self.snapshots_group_box)
        self.snapshots_layout.setSpacing(15)

        # --- Snapshots table ---
        self.snapshots_table = QtWidgets.QTableWidget(self.snapshots_group_box)
        self.snapshots_table.setColumnCount(3)
        self.snapshots_table.setHorizontalHeaderLabels(["Name", "Type", "Timestamp"])
        self.snapshots_table.horizontalHeader().setHighlightSections(False)
        self.snapshots_table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)
        self.snapshots_table.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
        self.snapshots_table.setSelectionMode(QtWidgets.QAbstractItemView.ExtendedSelection)
        self.snapshots_table.horizontalHeader().setSectionResizeMode(0, QtWidgets.QHeaderView.Stretch)
        self.snapshots_table.setColumnWidth(1, 60)
        self.snapshots_table.setColumnWidth(2, 150)
        self.snapshots_table.verticalHeader().setVisible(False)
        self.snapshots_table.setContextMenuPolicy(QtCore.Qt.CustomContextMenu)
        self.snapshots_table.customContextMenuRequested.connect(self.table_menu_event)
        self.snapshots_layout.addWidget(self.snapshots_table)

        # --- Context menu ---
        self.table_menu = QtWidgets.QMenu(self.form)

        self.view_action = QtWidgets.QAction("View", self.snapshots_group_box)
        self.view_action.setIcon(self._get_icon("open-file"))
        self.view_action.setDisabled(True)
        self.table_menu.addAction(self.view_action)

        self.delete_action = QtWidgets.QAction("Delete", self.snapshots_group_box)
        self.delete_action.setIcon(self._get_icon("delete"))
        self.delete_action.setDisabled(True)
        self.table_menu.addAction(self.delete_action)

        self.compare_action = QtWidgets.QAction("Compare", self.snapshots_group_box)
        self.compare_action.setIcon(self._get_icon("compare"))
        self.compare_action.setDisabled(True)
        self.table_menu.addAction(self.compare_action)

        self.table_menu.addSeparator()

        self.report_action = QtWidgets.QAction("Last Report", self.snapshots_group_box)
        self.report_action.setIcon(self._get_icon("xls"))
        self.report_action.setDisabled(True)
        self.table_menu.addAction(self.report_action)

        # --- Action buttons ---
        self.actions_layout = QtWidgets.QHBoxLayout()
        self.actions_layout.setSpacing(10)
        self.snapshots_layout.addLayout(self.actions_layout)

        self.report_button = QtWidgets.QPushButton("Last Report", self.snapshots_group_box)
        self.report_button.setIcon(self._get_icon("xls"))
        self.report_button.setIconSize(QtCore.QSize(25, 25))
        self.report_button.setMinimumSize(QtCore.QSize(150, 0))
        self.report_button.setDisabled(True)
        self.actions_layout.addWidget(self.report_button)

        self.folder_action = QtWidgets.QAction("Folder", self.snapshots_group_box)
        self.folder_action.setIcon(self._get_icon("opened-folder"))
        self.table_menu.addAction(self.folder_action)

        self.folder_button = QtWidgets.QPushButton("Folder", self.snapshots_group_box)
        self.folder_button.setIcon(self._get_icon("opened-folder"))
        self.folder_button.setIconSize(QtCore.QSize(25, 25))
        self.folder_button.setMinimumSize(QtCore.QSize(150, 0))
        self.actions_layout.addWidget(self.folder_button)

        self.actions_layout.addItem(QtWidgets.QSpacerItem(0, 0, QtWidgets.QSizePolicy.Expanding))

    def _get_icon(self, filename: str) -> QtGui.QIcon:
        """
        Load an icon from the assets directory.

        Args:
            filename (str): Name of the icon file (without extension).

        Returns:
            QtGui.QIcon: The QIcon object.
        """
        icon_path = os.path.join(os.path.dirname(__file__), "assets", f"{filename}.ico")
        icon = QtGui.QIcon()
        icon.addPixmap(QtGui.QPixmap(icon_path), QtGui.QIcon.Mode.Normal, QtGui.QIcon.State.Off)
        return icon

    def table_menu_event(self, pos):
        """
        Handles the custom context menu event for the snapshots table.

        Enables or disables the context menu actions based on the selected items.
        If one item is selected, enables 'View'.
        If exactly two items with different types are selected, enables 'Compare'.
        Always enables 'Delete' if any item is selected.

        Args:
            pos (QPoint): The position where the context menu is requested.
        """
        selected_items = self.get_selected_items()

        self.view_action.setDisabled(True)
        self.delete_action.setDisabled(True)
        self.compare_action.setDisabled(True)

        if selected_items:
            self.delete_action.setDisabled(False)

            if len(selected_items) < 2:
                self.view_action.setDisabled(False)

            if len(selected_items) == 2 and selected_items[0]['type'] != selected_items[1]['type']:
                self.compare_action.setDisabled(False)

        self.table_menu.exec_(self.snapshots_table.mapToGlobal(pos))

    def get_selected_items(self):
        """
        Retrieves selected items from the snapshots table and formats them as a list of dictionaries.

        Each dictionary contains:
            - 'name': Snapshot name
            - 'type': Snapshot type
            - 'timestamp': Snapshot timestamp
            - 'row': The row index of the snapshot in the table

        Returns:
            list: A list of dictionaries containing the selected snapshot information.
        """
        selected_data = []
        name = ''
        snapshot_type = ''
        timestamp = ''

        for item in self.snapshots_table.selectedItems():
            # Read the appropriate column data
            if item.column() == 0:
                name = item.text()
            elif item.column() == 1:
                snapshot_type = item.text()
            elif item.column() == 2:
                timestamp = item.text()

            # If all fields are collected, append a snapshot entry
            if name and snapshot_type and timestamp:
                selected_data.append({
                    'name': name,
                    'type': snapshot_type,
                    'timestamp': timestamp,
                    'row': item.row()
                })
                # Reset fields for the next snapshot
                name = ''
                snapshot_type = ''
                timestamp = ''

        return selected_data


class Form(QtWidgets.QWidget, Ui_Form):
    """
    UI Form class.

    """

    def __init__(self, parent=None, **kwargs):
        """
        Initialize the UI form.

        Args:
            parent (QWidget): Parent widget.
            **kwargs: Additional arguments for customization or metadata.
        """
        super().__init__(parent)
        self.kwargs = kwargs
        self.session = kwargs.get("session")
        self.output_dir = os.path.join(self.kwargs.get("output_dir"),
                                       os.path.basename(os.path.dirname(__file__).upper()))
        self.output_report = ""
        self.setup_ui(self)

        self.snapshots_table_row = 0
        logging.debug("Initializing snapshot scan on startup.")
        self.scan_snapshots()

        self.create_button.clicked.connect(self.create_start_event)
        self.view_action.triggered.connect(self.view_snapshot_event)
        self.delete_action.triggered.connect(self.delete_snapshot_event)
        self.compare_action.triggered.connect(self.compare_start_event)
        self.report_action.triggered.connect(lambda: self.open_path(self.output_report))
        self.report_button.clicked.connect(lambda: self.open_path(self.output_report))
        self.folder_action.triggered.connect(lambda: self.open_path(self.output_dir))
        self.folder_button.clicked.connect(lambda: self.open_path(self.output_dir))

    def scan_snapshots(self):
        """
        Scans the output directory for existing snapshots and adds them to the UI table.
        """
        snapshots_path = os.path.join(self.output_dir, 'Snapshots')

        if os.path.exists(snapshots_path):
            logging.debug(f"Scanning snapshots in: {snapshots_path}")
            for file in os.listdir(snapshots_path):
                try:
                    match = re.search(r'\[(.*)\]_\[(.*)\]_\[(.*)\].json', file)
                    if match:
                        snapshot = {
                            'timestamp': match.group(3),
                            'type': match.group(1),
                            'name': match.group(2)
                        }
                        self.add_snapshot(snapshot)
                except Exception as e:
                    logging.debug(f"Unable to add snapshot from file: {file} {e}")

    def create_start_event(self):
        """
        Starts the snapshot creation process by validating input and launching the background worker.
        """
        if not self.name_line_edit.text():
            QtWidgets.QMessageBox.critical(self, 'Error', 'Name of snapshot cannot be empty!')
            self.name_line_edit.setFocus()
            return

        logging.debug("Starting snapshot creation process.")
        self.create_button.setEnabled(False)
        self.create_worker = CreateEvent(self)
        self.create_worker.start()
        self.create_worker.add_snapshot_signal.connect(self.add_snapshot)
        self.create_worker.finished.connect(self.create_finish_event)

    def create_finish_event(self):
        """
        Callback for when the snapshot creation process finishes.
        """
        logging.debug("Snapshot creation finished.")
        self.create_button.setEnabled(True)

    def compare_start_event(self):
        """
        Starts the snapshot comparison process by launching the compare worker.
        """
        logging.debug("Starting snapshot comparison process.")
        self.compare_worker = CompareEvent(self)
        self.compare_worker.start()
        self.compare_worker.finished.connect(self.compare_finish_event)

    def compare_finish_event(self):
        """
        Handle the completion.
        """

        self.report_action.setDisabled(False)
        self.report_button.setDisabled(False)

        QtWidgets.QMessageBox.information(self, "Info", "Task completed!!")
        logging.debug("Diagnostics run completed successfully.")

    def add_snapshot(self, slot):
        """
        Adds a snapshot entry to the snapshots table.

        Args:
            slot (dict): A dictionary containing snapshot information with keys
                         'name', 'type', and 'timestamp'.
        """
        self.snapshots_table_row += 1
        row = self.snapshots_table.rowCount()
        self.snapshots_table.insertRow(row)
        self.snapshots_table.setItem(row, 0, QtWidgets.QTableWidgetItem(slot['name']))
        self.snapshots_table.setItem(row, 1, QtWidgets.QTableWidgetItem(slot['type']))
        self.snapshots_table.setItem(row, 2, QtWidgets.QTableWidgetItem(slot['timestamp']))
        logging.debug(f"Snapshot added: {slot['name']} - {slot['type']} - {slot['timestamp']}")

    def view_snapshot_event(self):
        """
        Opens selected snapshots by converting JSON data to an Excel workbook.
        """
        from netcore import XLBW
        data = self.get_selected_items()
        snapshots_path = os.path.join(self.output_dir, 'Snapshots')

        for row in data:
            file_name = f"[{row['type']}]_[{row['name']}]_[{row['timestamp']}].json"
            file_path = os.path.join(snapshots_path, file_name)

            if os.path.exists(file_path):
                with open(file_path, 'r') as f:
                    snapshot_data = json.load(f)

                handle, xlsx_file = mkstemp(suffix='.xlsx')
                workbook = XLBW(xlsx_file)
                workbook.dump(snapshot_data['endpoints'])
                workbook.close()

                logging.info(f"Snapshot opened: {row['name']}")
                self.open_path(xlsx_file)
            else:
                logging.warning(f"Snapshot file not found: {file_path}")

    def delete_snapshot_event(self):
        """
        Deletes selected snapshots from disk and removes them from the UI table.
        Ensures proper row index handling during multiple deletions.
        """
        data = self.get_selected_items()
        snapshots_path = os.path.join(self.output_dir, 'Snapshots')

        # Sort by row index in descending order to prevent index shift issues
        data.sort(key=lambda x: x['row'], reverse=True)

        for row in data:
            file_name = f"[{row['type']}]_[{row['name']}]_[{row['timestamp']}].json"
            file_path = os.path.join(snapshots_path, file_name)

            if os.path.exists(file_path):
                os.remove(file_path)
                self.snapshots_table.removeRow(row['row'])
                logging.info(f"Deleted snapshot: {row['name']}")
            else:
                logging.warning(f"Snapshot file not found for deletion: {file_path}")

    def open_path(self, path: str):
        """
        Open a file or directory using the system's default handler.

        Args:
            path (str): File or directory path to open.
        """
        try:
            if path and os.path.exists(path):
                logging.info(f"Opening path: {path}")
                QtGui.QDesktopServices.openUrl(QtCore.QUrl.fromLocalFile(path))
            else:
                logging.error(f"Invalid or non-existent path: {path}")
        except Exception as e:
            logging.exception(f"Failed to open path: {e}")
