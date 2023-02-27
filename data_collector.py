import os
import time
import glob
import datetime
import zipfile
from io import BytesIO
import requests as rq
import pandas as pd
import utils


LOGGER = utils.set_logger()


def collect_living_population_gu():
    """서울 생활인구 자치구 데이터 수집하기
    """
    LOGGER.info('=============================================================')
    LOGGER.info('데이터 수집 시작 - [ 생활인구 자치구 데이터 ]')
    LOGGER.info('=============================================================')

    url = 'https://datafile.seoul.go.kr/bigfile/iot/inf/nio_download.do?&useCache=false'
    headers = {'Host': 'datafile.seoul.go.kr',
               'Origin': 'https://data.seoul.go.kr',
               'Referer': 'https://data.seoul.go.kr/'}
    data = {'infId': 'OA-14979',
            'seq': '230222',
            'infSeq': '1'}

    lp_save_path = utils.check_path('생활인구_자치구')
    today = datetime.datetime.today()
    for x in range(21):
        new = today - datetime.timedelta(days=x)
        new_year = str(new.year)[2:]
        new_month = str(new.month).zfill(2)
        new_day = str(new.day).zfill(2)
        new = f'{new_year}{new_month}{new_day}'
        data['seq'] = new

        try:
            lp_file = rq.post(url, data, headers=headers)
            lp_file = zipfile.ZipFile(BytesIO(lp_file.content))
        except Exception:
            LOGGER.info(f'데이터 다운로드 실패 - 날짜: {new}')
            continue
        else:
            LOGGER.info(f'데이터 다운로드 성공 - 날짜: {new}')
            lp_file.extractall(lp_save_path)

        if len(glob.glob(os.path.join(lp_save_path, '*.csv'))) == 7:
            # 최근 일주일 데이터만 다운로드
            break
    LOGGER.info('데이터 수집 완료 - 생활인구 자치구 데이터')


def collect_living_population_dong():
    """서울 생활인구 행정동 데이터 수집하기
    """
    LOGGER.info('=============================================================')
    LOGGER.info('데이터 수집 시작 - [ 생활인구 행정동 데이터 ]')
    LOGGER.info('=============================================================')

    url = 'https://datafile.seoul.go.kr/bigfile/iot/inf/nio_download.do?&useCache=false'
    headers = {'Host': 'datafile.seoul.go.kr',
               'Origin': 'https://data.seoul.go.kr',
               'Referer': 'https://data.seoul.go.kr/'}
    data = {'infId': 'OA-14991',
            'seq': '2301',
            'infSeq': '3'}

    lp_save_path = utils.check_path('생활인구_행정동')
    today = datetime.datetime.today()
    for x in range(5):
        new = today - datetime.timedelta(days=30*x)
        new_year = str(new.year)[2:]
        new_month = str(new.month).zfill(2)
        new = f'{new_year}{new_month}'
        data['seq'] = new

        try:
            lp_file = rq.post(url, data, headers=headers)
            lp_file = zipfile.ZipFile(BytesIO(lp_file.content))
        except Exception:
            LOGGER.info(f'데이터 다운로드 실패 - 날짜: {new}')
            continue
        else:
            LOGGER.info(f'데이터 다운로드 성공 - 날짜: {new}')
            lp_file.extractall(lp_save_path)

        if len(glob.glob(os.path.join(lp_save_path, '*.csv'))) == 1:
            # 최근 1달 데이터만 다운로드
            break
    LOGGER.info('데이터 수집 완료 - 생활인구 행정동 데이터')


def collect_living_migration(is_gu):
    """서울 생활이동 자치구 (or 행정동) 데이터 수집하기
    """
    if is_gu:
        class_num = '3'
        class_str = '자치구'
    else:
        class_num = '2'
        class_str = '행정동'

    LOGGER.info('=============================================================')
    LOGGER.info(f'데이터 수집 시작 - [ 생활이동 {class_str} 데이터 ]')
    LOGGER.info('=============================================================')

    url = 'https://datafile.seoul.go.kr/bigfile/iot/inf/nio_download.do?&useCache=false'
    headers = {
        'Host': 'datafile.seoul.go.kr',
        'Origin': 'https://data.seoul.go.kr',
        'Referer': 'https://data.seoul.go.kr/',
    }

    # 코드 정보 다운로드
    code_data = {
        'infId': 'DOWNLOAD',
        'infSeq': '4',
        'seq': class_num
    }
    code_file = rq.post(url, code_data, headers=headers)
    code_file = pd.read_excel(BytesIO(code_file.content))
    save_path = utils.check_path(f'생활이동_{class_str}',
                                 f'생활이동_{class_str}_코드.csv')
    utils.save_data(save_path, code_file)
    LOGGER.info(f'데이터 수집 완료 - 생활이동 {class_str} 코드 정보')

    # 생활이동 데이터 다운로드
    now = time.localtime()
    year_month = []
    for x in range(5):
        new = time.localtime(time.mktime((now.tm_year, now.tm_mon - x, 1, 0, 0, 0, 0, 0, 0)))
        new = new[:2]
        new = f'{new[0]}{str(new[1]).zfill(2)}'
        year_month.append(new)

    for ym in year_month:
        lm_data = {
            'infId': 'DOWNLOAD',
            'infSeq': class_num,
            'seq': ym
        }
        try:
            lm_file = rq.post(url, lm_data, headers=headers)
            lm_file = zipfile.ZipFile(BytesIO(lm_file.content))
        except Exception:
            LOGGER.info(f'데이터 다운로드 실패 - 날짜: {ym}')
        else:
            zipinfo = lm_file.infolist()
            for info in zipinfo:
                info.filename = info.filename.encode('cp437').decode('euc-kr')
                lm_save_path = utils.check_path(f'생활이동_{class_str}',
                                                f'{class_str}_{ym}')
                lm_file.extract(info, lm_save_path)
            break
    LOGGER.info(f'데이터 수집 완료 - 생활이동 {class_str} 데이터 ({ym})')


if __name__ == '__main__':
    # # 생활이동 데이터
    # collect_living_migration(is_gu=True)   # 자치구
    # collect_living_migration(is_gu=False)  # 행정동

    # # 생활인구 데이터
    # collect_living_population_gu()
    collect_living_population_dong()
