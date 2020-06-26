import pandas as pd


def misma_carga_v2(df, concat_time=0.25):
    """
    La funcion concatena las cargas de combustible para cosmos

    Parameters
    ----------
    df : dataframe
        Contiene información acerca de las cargas de combustible
    concat_time : float
        DESCRIPTION. The default is 0.25. Define el tiempo segun el cual se
        consideran la misma carga de combustible

    Returns
    -------
    data_final : dataframe
        Contiene las cargas de combustible ya concatenadas segun el tiempo
        entre ellas

    """

    data = df.reset_index(drop=True)
    data_final = pd.DataFrame()

    for equipo in data.Equipo.unique():

        data_e = data[data["Equipo"] == equipo]
        data_e.sort_values(by=["Date"], inplace=True)
        data_e.reset_index(inplace=True)

        for i in range(len(data_e)):

            largo_data = len(data_e)

            if i < largo_data:

                if data_e.loc[i, "diff hrs"] < concat_time:

                    data_e.loc[i-1, "Cantidad"] =\
                        data_e.loc[i-1, "Cantidad"] + data_e.loc[i, "Cantidad"]

                    data_e = data_e.drop(i)

                    data_e.reset_index(inplace=True, drop=True)

        data_final = pd.concat([data_final, data_e])

    data_final.reset_index(inplace=True, drop=True)

    return data_final
