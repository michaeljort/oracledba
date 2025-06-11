import oracledb

class PDB:
    """Represents a Pluggable Database (PDB), either connected directly or managed by a CDB."""

    def __init__(self, name, cdb=None):
        self.name = name
        self.inherited_connection = False

        if cdb and cdb.connection:
            # Use the existing connection from CDB
            self.connection = cdb.connection
            self.cursor = self.connection.cursor()
            self.inherited_connection = True
            #self.set_container()
            self.cdb = cdb
        else:
            # Standalone PDB: Initialize its own connection
            from .cdb import CDB
            self.connect_params = CDB.from_yaml(name)
            self.connection = oracledb.connect(params=self.connect_params)
            self.cursor = self.connection.cursor()
            print(f"Standalone PDB {self.name} connected directly.")

            # Create a CDB object WITHOUT connecting
            self.cdb = CDB(self.get_cdb_name(), connect=False)

    def get_cdb_name(self):
        """Queries the database to determine the parent CDB name."""
        self.cursor.execute("SELECT name FROM V$DATABASE")
        return self.cursor.fetchone()[0]

    def set_container(self):
        """Switch session to the PDB when connected through a CDB."""
        if self.inherited_connection:
            self.cursor.execute(f"ALTER SESSION SET CONTAINER = {self.name}")
            print(f"Switched to PDB: {self.name}")

    def open_mode(self):
        """Retrieves the latest open mode of the PDB."""
        self.cursor.execute("SELECT OPEN_MODE FROM V$PDBS WHERE NAME = :pdb_name", pdb_name=self.name)
        result = self.cursor.fetchone()
        return result[0] if result else "UNKNOWN"

    def close(self):
        """Closes the PDB connection only if it was not inherited from a CDB."""
        self.cursor.close()
        if not self.inherited_connection:
            self.connection.close()
            print(f"PDB {self.name} connection closed.")
