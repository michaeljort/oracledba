import oracledb

class PDB:
    """Represents a Pluggable Database (PDB), either connected directly or managed by a CDB."""

    def __init__(self, name, cdb=None):
        self.name = name
        self._current_container = None  # Track the current container for session switching
        self.inherited_connection = False

        if cdb and cdb.connection:
            # Use the existing connection from CDB
            self.connection = cdb.connection
            self._cursor = self.connection.cursor()
            self.inherited_connection = True
            self.cdb = cdb
        else:
            # Standalone PDB: Initialize its own connection
            from .cdb import CDB
            self.connect_params = CDB.from_yaml(name)
            self.connection = oracledb.connect(params=self.connect_params)
            self._cursor = self.connection.cursor()
            print(f"Standalone PDB {self.name} connected directly.")

            # Create a CDB object WITHOUT connecting
            self.cdb = CDB(self.get_cdb_name(), connect=False)


    def execute(self, query, *args, **kwargs):
        # Executes a query in the correct PDB container
        if self.inherited_connection:
            self.set_container()
        return self._cursor.execute(query, *args, **kwargs)

    def fetchone(self):
        return self._cursor.fetchone()

    def fetchall(self):
        return self._cursor.fetchall()

    def get_cdb_name(self):
        """Queries the database to determine the parent CDB name."""
        self.execute("SELECT name FROM V$DATABASE")
        return self.fetchone()[0]

    def set_container(self):
        """Switch session to the PDB when connected through a CDB."""
        if self.inherited_connection and self._current_container != self.name:
            print(f"Switching to PDB: {self.name}")
            self._cursor.execute(f"ALTER SESSION SET CONTAINER = {self.name}")
            self._current_container = self.name

    def open_mode(self):
        """Retrieves the latest open mode of the PDB."""
        self.execute("SELECT OPEN_MODE FROM V$PDBS WHERE NAME = :pdb_name", pdb_name=self.name)
        result = self.fetchone()
        return result[0] if result else "UNKNOWN"

    def close(self):
        """Closes the PDB connection only if it was not inherited from a CDB."""
        self._cursor.close()
        if not self.inherited_connection:
            self.connection.close()
            print(f"PDB {self.name} connection closed.")
