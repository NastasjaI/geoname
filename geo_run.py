#!/usr/bin/env python
# coding: utf-8

# <h1>Table of Contents<span class="tocSkip"></span></h1>
# <div class="toc"><ul class="toc-item"></ul></div>

# In[1]:


from sqlalchemy import create_engine
import pandas as pd
from geopy.geocoders import Nominatim
from sklearn.metrics.pairwise import cosine_similarity
from fuzzywuzzy import fuzz


class GeoCities:


   
    def __init__(self, db_url):
        self.db_url = db_url # Подключение к базе данных.
        self.engine = create_engine(db_url)

    def load_table_from_db(self, table_name): # Загрузка данных из таблицы базы данных
        query = f"SELECT * FROM {table_name}"
        return pd.read_sql(query, self.engine)

    def merge_tables(self, tables): # Объединение таблиц (разные таблицы объединяются по разным ключам)
        merged_df = pd.merge(tables[0], tables[1], on='geonameid')

        merged_df = pd.merge(merged_df, tables[2], left_on='geonameid', right_on='code')
        merged_df = merged_df.drop('code', axis=1) 

        merged_df = pd.merge(merged_df, tables[3], left_on='country', right_on='country_code', how='left')
        merged_df = merged_df.drop('country_code', axis=1)  
        
        return merged_df

    def get_embedding(self, city): # Получение эмбеддинга для указанного города
        geolocator = Nominatim(user_agent="geo_locator")
        location = geolocator.geocode(city)
        if location:
            return [location.latitude, location.longitude]
        else:
            return [0, 0]

    def find_similar_cities(self, target_city, city_list, threshold=80): #Поиск похожих городов для заданного города
        similar_cities = []

        for _, row in city_list.iterrows():
            city_name = row['name']
            if pd.notna(city_name) and isinstance(city_name, str):
                similarity = fuzz.ratio(target_city, city_name)

                if similarity >= threshold:
                    # Вычисление косинусного расстояния
                    target_embedding = self.get_embedding(target_city)
                    city_embedding = self.get_embedding(city_name)
                    cosine_sim = cosine_similarity([target_embedding], [city_embedding])[0][0]

                    # Формирование словаря
                    city_info = {
                        'geonameid': row['geonameid'],
                        'name': city_name,
                        'region': row['region'],
                        'country': row['country'],
                        'cosine_similarity': cosine_sim
                    }

                    similar_cities.append(city_info)

        return similar_cities

    def run_analysis(self, target_city): # Запуск анализа похожих городов для заданного города
        countries = self.load_table_from_db('countries')
        cities = self.load_table_from_db('cities')
        admin_codes = self.load_table_from_db('admin_codes')
        alternatenames = self.load_table_from_db('alternatenames')

        tables = [cities, alternatenames.drop(['alternateNameId', 'name'], axis=1),
                  admin_codes.drop('geonameid', axis=1), countries.drop('geonameid', axis=1)]

        merged_data = self.merge_tables(tables)
        city_list = merged_data[['geonameid', 'name', 'region', 'country']]

        similar_cities = self.find_similar_cities(target_city, city_list)

        # Вывод результата
        for city_info in similar_cities:
            print(city_info)


# In[ ]:




