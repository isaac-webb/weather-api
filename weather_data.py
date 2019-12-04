import requests
import numpy as np
import pandas as pd
import json
import datetime
from google.cloud import bigquery


class WeatherDataAPI:
    """
    Class that provides a set of methods to interact with NOAA's historic
    climate data REST API.
    """
    BASE_URL = 'https://www.ncei.noaa.gov/access/services/data/v1'
    ATL_STATION_CODE = 'USW00013874'

    @staticmethod
    def get_weather_data(start_date: datetime.date, end_date: datetime.date,
                         data_type: str, station: str = ATL_STATION_CODE):
        """
        This function provides the ability to request data in a certain
        time range from a NOAA GHCN station. The result is a pandas
        DataFrame.

        :param start_date: the first day desired in the data set
        :param end_date: the last day desired in the data set
        :param data_type: the desired data type, e.g. 'TAVG', 'TMIN', or 'TMAX'
        :param station: the station to source data from
        :return: a pandas DataFrame containing the requested data
        :raises Exception: raises an exception when the request fails
        """
        # Request the data from the API
        body = requests.get(WeatherDataAPI.BASE_URL, params={
            'dataset': 'daily-summaries',
            'dataTypes': data_type,
            'stations': station,
            'startDate': start_date.isoformat(),
            'endDate': end_date.isoformat(),
            'format': 'json'
        })

        # Check the response status
        if body.status_code == 200:
            # If we successfully retrieved the data, convert it into a
            # pandas DataFrame and return it
            return pd.DataFrame(json.loads(body.content))
        else:
            # If the request failed, throw an exception
            raise Exception(f"Error when requesting data: {body.content}")

    @staticmethod
    def get_min_temps(start_date: datetime.date, end_date: datetime.date,
                      station: str = ATL_STATION_CODE):
        # Get the minimum temperatures from the API
        results = WeatherDataAPI.get_weather_data(start_date, end_date,
                                                  'TMIN', station)

        # Convert the temperatures to fahrenheit and index the dataset
        # using the date
        df = pd.DataFrame()
        df['date'] = results['DATE'].astype('datetime64')
        df['minTemp'] = results['TMIN'].astype('float64') / 10 * 9 / 5 + 32
        df = df.set_index(['date'])
        return df

    @staticmethod
    def get_max_temps(start_date: datetime.date, end_date: datetime.date,
                      station: str = ATL_STATION_CODE):
        # Get the maximum temperatures from the API
        results = WeatherDataAPI.get_weather_data(start_date, end_date,
                                                  'TMAX', station)

        # Convert the temperatures to fahrenheit and index the dataset
        # using the date
        df = pd.DataFrame()
        df['date'] = results['DATE'].astype('datetime64')
        df['maxTemp'] = results['TMAX'].astype('float64') / 10 * 9 / 5 + 32
        df = df.set_index(['date'])
        return df

    @staticmethod
    def get_avg_temps(start_date: datetime.date, end_date: datetime.date,
                      station: str = ATL_STATION_CODE):
        # Get the average temperatures from the API
        results = WeatherDataAPI.get_weather_data(start_date, end_date,
                                                  'TAVG', station)

        # Convert the temperatures to fahrenheit and index the dataset
        # using the date
        df = pd.DataFrame()
        df['date'] = results['DATE'].astype('datetime64')
        df['avgTemp'] = results['TAVG'].astype('float64') / 10 * 9 / 5 + 32
        df = df.set_index(['date'])
        return df

    @staticmethod
    def get_daily_temps(start_date: datetime.date, end_date: datetime.date,
                        station: str = ATL_STATION_CODE):
        # Get the minimum, maximum, and average temperatures from the API
        min_temps = WeatherDataAPI.get_min_temps(start_date, end_date, station)
        max_temps = WeatherDataAPI.get_max_temps(start_date, end_date, station)
        avg_temps = WeatherDataAPI.get_avg_temps(start_date, end_date, station)

        # Combine them and return the result
        return pd.concat([min_temps, max_temps, avg_temps], axis=1)


class BigQueryWeatherDataAPI:
    ATL_STATION_CODE = 'USW00013874'
    CLIENT = bigquery.Client()

    @staticmethod
    def get_weather_data(start_date: datetime.date, end_date: datetime.date,
                         data_type: str, station: str = ATL_STATION_CODE):
        # Generate the SQL query using the provided parameters
        query = (
            f"SELECT wx.date AS date, wx.value/10.0 AS {data_type.lower()} "
            f"FROM `bigquery-public-data.ghcn_d.ghcnd_{start_date.year}` "
            f"AS wx WHERE id = '{station}' AND qflag IS NULL "
            f"AND element = '{data_type}' "
            f"AND wx.date>='{start_date}' AND wx.date<='{end_date}' "
            f"ORDER BY wx.date"
        )

        # Query the API and return the result
        df = BigQueryWeatherDataAPI.CLIENT.query(query).to_dataframe()
        return df

    @staticmethod
    def get_min_temps(start_date: datetime.date, end_date: datetime.date,
                      station: str = ATL_STATION_CODE):
        # Get the minimum temperatures from the API
        results = BigQueryWeatherDataAPI.get_weather_data(start_date, end_date,
                                                          'TMIN', station)

        # Convert the temperatures to fahrenheit and index the dataset
        # using the date
        df = pd.DataFrame()
        df['date'] = results['date'].astype('datetime64[ns]')
        df['minTemp'] = results['tmin'].astype('float64') * 9 / 5 + 32
        df = df.set_index(['date'])
        return df

    @staticmethod
    def get_max_temps(start_date: datetime.date, end_date: datetime.date,
                      station: str = ATL_STATION_CODE):
        # Get the maximum temperatures from the API
        results = BigQueryWeatherDataAPI.get_weather_data(start_date, end_date,
                                                          'TMAX', station)

        # Convert the temperatures to fahrenheit and index the dataset
        # using the date
        df = pd.DataFrame()
        df['date'] = results['date'].astype('datetime64[ns]')
        df['maxTemp'] = results['tmax'].astype('float64') * 9 / 5 + 32
        df = df.set_index(['date'])
        return df

    @staticmethod
    def get_avg_temps(start_date: datetime.date, end_date: datetime.date,
                      station: str = ATL_STATION_CODE):
        # Get the average temperatures from the API
        results = BigQueryWeatherDataAPI.get_weather_data(start_date, end_date,
                                                          'TAVG', station)

        # Convert the temperatures to fahrenheit and index the dataset
        # using the date
        df = pd.DataFrame()
        df['date'] = results['date'].astype('datetime64[ns]')
        df['avgTemp'] = results['tavg'].astype('float64') * 9 / 5 + 32
        df = df.set_index(['date'])
        return df

    @staticmethod
    def get_daily_temps(start_date: datetime.date, end_date: datetime.date, station: str = ATL_STATION_CODE):
        # Get the minimum, maximum, and average temperatures from the API
        min_temps = BigQueryWeatherDataAPI.get_min_temps(start_date, end_date, station)
        max_temps = BigQueryWeatherDataAPI.get_max_temps(start_date, end_date, station)
        avg_temps = BigQueryWeatherDataAPI.get_avg_temps(start_date, end_date, station)

        # Combine them and return the result
        return pd.concat([min_temps, max_temps, avg_temps], axis=1)


def main():
    print(BigQueryWeatherDataAPI.get_daily_temps(
              datetime.date.today() - datetime.timedelta(days=30),
              datetime.date.today()
          ))


if __name__ == '__main__':
    main()
