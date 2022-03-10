from shapefile import Reader
from pandas import DataFrame


def read_shapefile(path):
    """
    Read a shapefile into a Pandas dataframe with a 'coords' column holding
    the geometry information
    :param path: path to zipped shapefile
    :return: Pandas DataFrame
    """
    sf = Reader(path)
    fields = [x[0] for x in sf.fields][1:]
    records = sf.records()
    shps = [s.points for s in sf.shapes()]

    df = DataFrame(columns=fields, data=records)
    df = df.assign(coords=shps)

    return df
