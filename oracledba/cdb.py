import os
import yaml
import oracledb
import inspect
from .pdb import PDB

class CDB:
    """Represents a Container Database (CDB) that dynamically loads its PDBs from the database."""

    def __init__(self, db_name, connect=True):
        """Initializes a CDB. If 'connect' is False, only metadata is assigned."""
        self.pdbs = []

        if connect:
            self.connect_params = self.from_yaml(db_name)
            self.connection = oracledb.connect(params=self.connect_params)
            self.cursor = self.connection.cursor()
            self.pdbs = self.discover_pdbs()  # Automatically load PDBs
            print(f"Connected to '{db_name}'")
            print(f"Discovered PDBs: {', '.join([pdb.name for pdb in self.pdbs])}")

    @staticmethod
    def from_yaml(db_name):
        """Loads connection parameters from oracledba.yaml."""
        search_paths = [
            os.getenv("TNS_ADMIN"),
            os.path.join(os.getenv("ORACLE_HOME", ""), "network", "admin"),
            os.path.dirname(os.path.abspath(__file__))
        ]

        config = None
        for path in search_paths:
            if path and os.path.exists(path):
                config_path = os.path.join(path, "oracledba.yaml")
                if os.path.exists(config_path):
                    with open(config_path, "r") as f:
                        config = yaml.safe_load(f)
                    break

        if config is None:
            raise FileNotFoundError("No oracledba.yaml found in expected locations.")

        db_params = config.get("databases", {}).get(db_name)
        if not db_params:
            raise ValueError(f"No configuration found for database: {db_name}")

        valid_params = {k: v for k, v in db_params.items() if v is not None and k != "connection_string"}
        connect_params = oracledb.ConnectParams(**valid_params)

        if "connection_string" in db_params and db_params["connection_string"]:
            connect_params.parse_connect_string(db_params["connection_string"])

        return connect_params

    def get_cdb_name(self):
        """Queries the database to determine the parent CDB name."""
        self.cursor.execute("SELECT name FROM V$DATABASE")
        return self.cursor.fetchone()[0]

    def discover_pdbs(self):
        """Queries the database to dynamically retrieve all available PDBs."""
        if not hasattr(self, "cursor"):  # Avoid querying if no connection was established
            return []
        self.cursor.execute("SELECT NAME FROM V$PDBS")
        pdb_names = [row[0] for row in self.cursor.fetchall()]
        return [PDB(name, cdb=self) for name in pdb_names]

    def close(self):
        """Closes the connection and all registered PDBs."""
        for pdb in self.pdbs:
            pdb.close()
        if hasattr(self, "cursor"):
            self.cursor.close()
        if hasattr(self, "connection"):
            self.connection.close()
            print("CDB and all PDB connections closed.")
