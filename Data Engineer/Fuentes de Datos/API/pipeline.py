import os
import requests
import pandas as pd

# https://dashboard.api-football.com/register

class Pipeline:
    def __init__(self, country, output_folder):
        self.country = country
        self.output_folder = output_folder
        self.api_key = 'a1122b94efd6efb0f530829e96fe3bac'
        self.data = None

    def extract(self):
        url = f'https://v3.football.api-sports.io/teams?country={self.country}'
        headers = {
            'x-apisports-key': self.api_key,
            'x-apisports-host': 'v3.football.api-sports.io'
        }
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            self.data = response.json()
        else:
            raise Exception(f"Error al obtener los datos: {response.status_code}")

    def transform(self):
        if self.data is None:
            raise Exception("No hay datos para transformar. Ejecute el método extract() primero.")
        df = pd.json_normalize(self.data['response'])
        df = df[[
            'team.name', 'team.code', 'team.founded', 'team.logo',
            'venue.name', 'venue.address', 'venue.city', 'venue.capacity', 'venue.image', 'team.national'
        ]]
        df = df.loc[df['team.national'] == False]
        df.drop(columns=['team.national'], inplace=True)
        df.columns = [col.replace(".", "_") for col in df.columns]
        df['team_name'] = df['team_name'].str.upper()
        current_year = pd.Timestamp.now().year
        df['team_age'] = current_year - df['team_founded']
        self.data = df

    def load(self):
        if self.data is None or not isinstance(self.data, pd.DataFrame):
            raise Exception("No hay datos para cargar. Ejecute el método transform() primero.")
        if not os.path.exists(self.output_folder):
            os.makedirs(self.output_folder)
        output_path = os.path.join(self.output_folder, f'{self.country}_teams.csv')
        self.data.to_csv(output_path, index=False)

    def run(self):
        self.extract()
        self.transform()
        self.load()
