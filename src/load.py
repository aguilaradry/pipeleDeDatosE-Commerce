from typing import Dict

from pandas import DataFrame
from sqlalchemy.engine.base import Engine


def load(data_frames: Dict[str, DataFrame], database: Engine):
    """Load the dataframes into the sqlite database.

    Args:
        data_frames (Dict[str, DataFrame]): A dictionary with keys as the table names
        and values as the dataframes.
    """
    # TODO: Implementa esta función. Por cada DataFrame en el diccionario, debes
    # usar pandas.DataFrame.to_sql() para cargar el DataFrame en la base de datos
    # como una tabla.
    # Para el nombre de la tabla, utiliza las claves del diccionario `data_frames`.
    
        # Elimina las tablas existentes antes de cargarlas
    with database.connect() as conn:
        for table_name in data_frames.keys():
            try:
                conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                print(f"Tabla '{table_name}' eliminada correctamente.")
            except Exception as e:
                print(f"Error al eliminar la tabla '{table_name}': {e}")

    # Carga los DataFrames
    for key, value in data_frames.items():
        try:
            value.to_sql(key, con=database, if_exists='replace', index=False)
            print(f"Tabla '{key}' cargada exitosamente.")
        except Exception as e:
            print(f"Error al cargar la tabla '{key}': {e}")