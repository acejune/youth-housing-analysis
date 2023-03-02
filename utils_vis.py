import os
import sys
import logging
import json
import time
import glob
import pandas as pd
import numpy as np
import geopandas as gpd
import folium
import requests
from tqdm import tqdm
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
import utils


LOGGER = utils.set_logger()


def process_geo_data():
    """시각화에 필요한 geo 데이터 처리하기
    """
    file_path = os.path.join(utils.check_path('행정동'), '*.json')
    file_path_list = glob.glob(file_path)
    file_path = [f for f in file_path_list if '경계' in f][0]

    geo_df = gpd.read_file(file_path)
    geo_df = geo_df[['adm_cd8', 'geometry']]
    geo_df.columns = ['dong_code', 'geometry']

    geo_json = utils.load_data(file_path)
    geo_json['features'] = [i for i in geo_json['features'] if i['properties']['sidonm'] == '서울특별시']

    return geo_df, geo_json


def save_folium_image(folium_map, file_name_no_ext):
    """folium으로 시각화 한 파일 저장하기
    """
    # html_title = f'<h2 style="text-align: center;">{file_name_no_ext}</h2>'
    # folium_map.get_root().html.add_child(folium.Element(html_title))

    html_dir = os.path.join(os.path.abspath('.'), 'report', 'vis_html')
    if not os.path.exists(html_dir):
        os.makedirs(html_dir)
    html_path = os.path.join(html_dir, f'{file_name_no_ext}.html')
    folium_map.save(html_path)

    url = f'file://{html_path}'
    browser = webdriver.Chrome(service=Service(ChromeDriverManager().install()),
                               options=webdriver.ChromeOptions())
    browser.get(url)
    time.sleep(1)
    image_path = utils.check_path('시각화', f'{file_name_no_ext}.png')
    browser.save_screenshot(image_path)
    browser.close()
    LOGGER.info(f'folium 이미지 저장 완료 - {file_name_no_ext}.png')


def visualize_by_dong(df, columns, aliases_columns, legend_name):
    """folium으로 행정동 시각화하기
    """
    null_df = df.replace(0, np.NaN)

    geo_df, geo_json = process_geo_data()
    df = geo_df.merge(df, on='dong_code')

    seoul_map = folium.Map(location=[37.5726, 126.9740],
                           zoom_start=11,
                           tiles='cartodbpositron')

    folium.Choropleth(geo_data=geo_json,
                      data=null_df,
                      columns=columns,
                      key_on='feature.properties.adm_cd8',
                      fill_color='YlOrRd',
                      bins=253,
                      fill_opacity=0.6,
                      nan_fill_color='white',
                      nan_fill_opacity=0.6,
                      highlight=True,
                      legend_name=legend_name).add_to(seoul_map)

    columns[0] = 'dong_name'
    folium.features.GeoJson(data=df,
                            smooth_factor=2,
                            style_function=lambda x: {'color':'black', 'fillColor':'transparent', 'weight':0.5},
                            tooltip=folium.features.GeoJsonTooltip(
                                fields=columns,
                                aliases=aliases_columns,
                                localize=True,
                                sticky=False,
                                labels=True,
                                style="""
                                    background-color: #F0EFEF;
                                    border: 2px solid black;
                                    border-radius: 3px;
                                    box-shadow: 3px;
                                    """,
                                max_width=800,),
                            highlight_function=lambda x: {'weight':3, 'fillColor':'grey'},
                            ).add_to(seoul_map)

    save_folium_image(folium_map=seoul_map,
                      file_name_no_ext=legend_name)
