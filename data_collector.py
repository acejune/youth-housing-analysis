import datetime
import glob
import json
import os
import re
import time
import zipfile
from io import BytesIO

import pandas as pd
import requests as rq
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from tqdm import tqdm
from webdriver_manager.chrome import ChromeDriverManager

import utils

LOGGER = utils.set_logger()


def collect_living_population_dong():
    """서울 생활인구 행정동 데이터 수집하기"""
    LOGGER.info("=============================================================")
    LOGGER.info("데이터 수집 시작 - [ 생활인구 행정동 데이터 ]")
    LOGGER.info("=============================================================")

    url = "https://datafile.seoul.go.kr/bigfile/iot/inf/nio_download.do?&useCache=false"
    headers = {
        "Host": "datafile.seoul.go.kr",
        "Origin": "https://data.seoul.go.kr",
        "Referer": "https://data.seoul.go.kr/",
    }

    # 행정동 코드 정보 다운로드
    dongcode_data = {"infId": "DOWNLOAD", "infSeq": "4", "seq": "7"}
    dongcode_file = rq.post(url, dongcode_data, headers=headers)
    dongcode_file = pd.read_excel(BytesIO(dongcode_file.content))
    dongcode_save_path = utils.check_path("생활인구", "생활인구_행정동_코드_raw.csv")
    utils.save_data(dongcode_save_path, dongcode_file, encoding="cp949")
    LOGGER.info("데이터 수집 완료 - 생활인구 행정동 코드 정보")

    # 생활인구 데이터 다운로드
    lp_save_path = utils.check_path("생활인구")
    lp_data = {"infId": "OA-14991", "seq": "2301", "infSeq": "3"}
    today = datetime.datetime.today()
    for x in range(5):
        new = today - datetime.timedelta(days=30 * x)
        new_year = str(new.year)[2:]
        new_month = str(new.month).zfill(2)
        new = f"{new_year}{new_month}"
        lp_data["seq"] = new

        try:
            lp_file = rq.post(url, lp_data, headers=headers)
            lp_file = zipfile.ZipFile(BytesIO(lp_file.content))
        except Exception:
            LOGGER.info(f"데이터 다운로드 실패 - 날짜: {new}")
            continue
        else:
            LOGGER.info(f"데이터 다운로드 성공 - 날짜: {new}")
            info = lp_file.infolist()[0]
            info.filename = f"생활인구_데이터_{new}_행정동_raw.csv"
            lp_file.extract(info, lp_save_path)

        if len(glob.glob(os.path.join(lp_save_path, "*.csv"))) == 2:
            # 최근 1달 데이터만 다운로드 (행정동_코드.csv, lp_data.csv)
            break
    LOGGER.info(f"데이터 수집 완료 - 생활인구 행정동 데이터 ({new})")


def collect_living_migration_dong():
    """서울 생활이동 행정동 데이터 수집하기"""
    LOGGER.info("=============================================================")
    LOGGER.info("데이터 수집 시작 - [ 생활이동 행정동 데이터 ]")
    LOGGER.info("=============================================================")

    url = "https://datafile.seoul.go.kr/bigfile/iot/inf/nio_download.do?&useCache=false"
    headers = {
        "Host": "datafile.seoul.go.kr",
        "Origin": "https://data.seoul.go.kr",
        "Referer": "https://data.seoul.go.kr/",
    }

    # 행정동코드 정보 다운로드
    dongcode_data = {"infId": "DOWNLOAD", "infSeq": "4", "seq": "2"}
    dongcode_file = rq.post(url, dongcode_data, headers=headers)
    dongcode_file = pd.read_excel(BytesIO(dongcode_file.content))
    dongcode_save_path = utils.check_path("생활이동", "생활이동_행정동_코드_raw.csv")
    utils.save_data(dongcode_save_path, dongcode_file, encoding="cp949")
    LOGGER.info("데이터 수집 완료 - 생활이동 행정동 코드 정보")

    # 생활이동 데이터 다운로드
    now = time.localtime()
    year_month = []
    for x in range(5):
        new = time.localtime(
            time.mktime((now.tm_year, now.tm_mon - x, 1, 0, 0, 0, 0, 0, 0))
        )
        new = new[:2]
        new = f"{new[0]}{str(new[1]).zfill(2)}"
        year_month.append(new)

    for ym in year_month:
        lm_data = {"infId": "DOWNLOAD", "infSeq": "2", "seq": ym}
        try:
            lm_file = rq.post(url, lm_data, headers=headers)
            lm_file = zipfile.ZipFile(BytesIO(lm_file.content))
        except Exception:
            LOGGER.info(f"데이터 다운로드 실패 - 날짜: {ym}")
        else:
            zipinfo = lm_file.infolist()
            for info in zipinfo:
                info.filename = info.filename.encode("cp437").decode("euc-kr")
                lm_save_path = utils.check_path("생활이동", f"생활이동_데이터_{ym}_행정동_raw")
                lm_file.extract(info, lm_save_path)
            break
    LOGGER.info(f"데이터 수집 완료 - 생활이동 행정동 데이터 ({ym})")


def collect_latest_dong_code():
    """최신 행정동 코드 데이터 수집하기"""
    LOGGER.info("=============================================================")
    LOGGER.info("데이터 수집 시작 - [ 최신 행정동 코드 데이터 ]")
    LOGGER.info("=============================================================")

    url = (
        "http://kssc.kostat.go.kr/ksscNew_web/kssc/common/AdCodeConnectionSearchList.do"
    )
    browser = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=webdriver.ChromeOptions(),
    )
    browser.get(url)

    # 데이터 버전 (업데이트 날짜) 확인
    table = browser.find_element(By.CLASS_NAME, "tbl_type3")
    thead = table.find_element(By.TAG_NAME, "thead")
    tr = thead.find_elements(By.TAG_NAME, "tr")[-1]
    version = tr.find_elements(By.TAG_NAME, "th")[0].get_attribute("innerText")
    version = version.replace(".", "-")

    name2code = {}
    for i in tqdm(range(25)):  # 서울시 자치구 개수 = 25
        Select(
            browser.find_element(By.ID, "strHighCategoryCode")
        ).select_by_visible_text("서울특별시")
        time.sleep(1)
        Select(browser.find_element(By.ID, "strCategoryCode")).select_by_index(i + 1)
        time.sleep(1)
        Select(browser.find_element(By.ID, "strSearchGugun")).select_by_visible_text(
            "행정동"
        )
        browser.find_element(By.XPATH, '//button[text()="검색"]').click()
        time.sleep(10)

        table = browser.find_element(By.CLASS_NAME, "tbl_type3")
        tbody = table.find_element(By.TAG_NAME, "tbody")
        rows = tbody.find_elements(By.TAG_NAME, "tr")
        gu = (
            rows[0]
            .find_elements(By.TAG_NAME, "td")[2]
            .get_attribute("innerText")
            .strip()
        )
        for row in rows[1:]:
            tds = row.find_elements(By.TAG_NAME, "td")
            code = tds[1].get_attribute("innerText").strip()
            dong = tds[2].get_attribute("innerText").strip()
            name2code[f"서울 {gu} {dong}"] = code
    browser.close()
    name2code = dict(sorted(name2code.items(), key=lambda x: x[0]))

    # 데이터 저장
    save_path = utils.check_path("행정동", f"행정동_코드_v{version}.json")
    utils.save_data(save_path, name2code)
    LOGGER.info(f"데이터 수집 완료 - 최신 행정동 코드 데이터 (ver. {version})")


def collect_dong_boundary():
    """행정동 경계 데이터 수집하기"""
    LOGGER.info("=============================================================")
    LOGGER.info("데이터 수집 시작 - [ 행정동 경계 데이터 ]")
    LOGGER.info("=============================================================")

    url = "https://raw.githubusercontent.com/vuski/admdongkor/master/ver20230101/HangJeongDong_ver20230101.geojson"
    response = rq.get(url)
    response = response.text
    data = json.loads(response)

    version = re.search(r"(?<=ver)\d+", data["name"]).group()
    version = f"{version[:4]}-{version[4:6]}-{version[6:]}"

    save_path = utils.check_path("행정동", f"행정동_경계_v{version}.json")
    utils.save_data(save_path, data)
    LOGGER.info(f"데이터 수집 완료 - 행정동 경계 데이터 (ver. {version})")


def collect_youth_housing_in_station_area():
    """역세권 청년주택 데이터 수집하기"""
    base_url = "https://soco.seoul.go.kr/youth/main/main.do"

    browser = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=webdriver.ChromeOptions(),
    )
    browser.get(base_url)
    raw_html_list = browser.find_elements(By.CLASS_NAME, "slick-slide")

    house_id_list = []
    ptr = re.compile(r"homeView\(([\d]+)\)")
    for raw_html in raw_html_list:
        try:
            html_str = raw_html.get_attribute("innerHTML")
            house_id = ptr.search(html_str)
            house_id = house_id.group(1)
            house_id_list.append(house_id)
        except Exception:
            continue
    browser.close()

    house_list = []
    for house_id in house_id_list:
        url = "https://soco.seoul.go.kr/youth/pgm/home/yohome/view.do"
        data = {"menuNo": "400002", "homeCode": house_id}
        headers = {"User-Agent": "Mozilla/5.0"}
        response = rq.post(url, data=data, headers=headers)
        html = response.text
        soup = BeautifulSoup(html, "html.parser")
        dashline_list = soup.find_all(class_="dashline")

        house_info = []
        for dashline in dashline_list:
            house_info.extend(dashline.find_all("p"))

        ptr = re.compile(r"(<.*?>|\s{2,})")
        for index, info in enumerate(house_info):
            info = re.sub(ptr, "", str(info))
            house_info[index] = info

        house_info = " /// ".join(house_info)
        ptr_address = re.compile(r"주소 : ([가-힣\s\d]+)")
        ptr_ho = re.compile(r"총 ([가-힣\s\d]+호)")
        ptr_sil = re.compile(r"총 ([가-힣\s\d]+실)")

        address = ptr_address.search(house_info)
        address = address.group(1).strip()
        address = address.replace("서울특별시", "서울")
        ho = ptr_ho.search(house_info)
        ho = ho.group(1).strip()
        sil = ptr_sil.search(house_info)
        sil = sil.group(1).strip()
        house_list.append((address, ho, sil))
    df = pd.DataFrame(house_list, columns=["address", "ho", "sil"])

    save_path = utils.check_path("청년주택", "역세권_청년주택_raw.csv")
    utils.save_data(save_path, df, encoding="cp949")
    LOGGER.info("데이터 수집 완료 - 역세권 청년주택 데이터")


if __name__ == "__main__":
    # # 생활인구 데이터
    # collect_living_population_dong()

    # # 생활이동 데이터
    # collect_living_migration_dong()

    # # 최신 행정동 코드 데이터
    # collect_latest_dong_code()

    # # 행정동 경계 데이터
    # collect_dong_boundary()

    # 역세권 청년 주택 데이터
    collect_youth_housing_in_station_area()
