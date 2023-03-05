import glob
import os
from collections import defaultdict

import pandas as pd

import utils
import utils_vis

LOGGER = utils.set_logger()


def process_living_population_dong_code():
    """'생활인구 행정동 코드' 전처리하기

    NOTE:
        - (과거) 강남구 일원2동(11230740) → (현재) 강남구 개포3동(11230511)
        - (과거) 강동구 상일동(11250520) → (현재) 강동구 상일1동(11250760)
        - (과거) 강동구 강일동(11250510) → (현재) 강동구 강일동(11250750), 강동구 상일2동(11250770)
        - (과거) 구로구 오류2동(11170680) → (현재) 구로구 오류2동(11170730), 구로구 항동(11170740)
    """
    LOGGER.info("=============================================================")
    LOGGER.info("데이터 처리 시작 - [ 생활인구 행정동 코드 ]")
    LOGGER.info("=============================================================")

    raw_code_file_path = utils.check_path("생활인구", "생활인구_행정동_코드_raw.csv")
    df = utils.load_data(raw_code_file_path, encoding="cp949")
    df = df.drop([0])

    lp_name2code = {}
    lp_hcode2scode = {}
    for row in df.itertuples():
        dong = f"{row.시도명} {row.시군구명} {row.행정동명}"
        dong = dong.replace(".", "·")
        s_code = row.통계청행정동코드 + "0"
        h_code = row.행자부행정동코드
        lp_name2code[dong] = s_code
        lp_hcode2scode[h_code] = s_code

    # 행정동 이름과 (통계청) 행정동 코드 데이터 저장
    lp_name2code = dict(sorted(lp_name2code.items(), key=lambda x: x[0]))
    lp_name2code_save_path = utils.check_path("생활인구", "생활인구_행정동_코드.json")
    utils.save_data(lp_name2code_save_path, lp_name2code)
    LOGGER.info("데이터 저장 완료 - 행정동 이름 to 행정동 코드")

    # 생활인구 데이터에서 사용하는 (행자부) 행정동 코드와 (통계청) 행정동 코드 데이터 저장
    lp_hcode2scode = dict(sorted(lp_hcode2scode.items(), key=lambda x: x[0]))
    lp_hcode2scode_save_path = utils.check_path("생활인구", "생활인구_행정동_코드_h2s.json")
    utils.save_data(lp_hcode2scode_save_path, lp_hcode2scode)
    LOGGER.info("데이터 저장 완료 - (행자부) 행정동 코드 to (통계청) 행정동 코드")

    # '최신 행정동 코드'와 비교
    lp_name2code = set(lp_name2code.items())
    latest_name2code_path = os.path.join(utils.check_path("행정동"), "행정동_코드_*")
    latest_name2code_path = glob.glob(latest_name2code_path)[0]
    latest_name2code = utils.load_data(latest_name2code_path)
    latest_name2code = set(latest_name2code.items())
    LOGGER.info(f"(최신 행정동) - (생활인구 행정동) = {latest_name2code - lp_name2code}")
    LOGGER.info(f"(생활인구 행정동) - (최신 행정동) = {lp_name2code - latest_name2code}")


def process_living_population_data():
    """생활인구 데이터 전처리하기"""
    LOGGER.info("=============================================================")
    LOGGER.info("데이터 처리 시작 - [ 생활인구 데이터 ]")
    LOGGER.info("=============================================================")

    data_file_path = os.path.join(utils.check_path("생활인구"), "*.csv")
    data_file_path_list = glob.glob(data_file_path)
    data_file_path = [f for f in data_file_path_list if "생활인구_데이터" in f][0]
    df = utils.load_data(data_file_path)

    df = df.reset_index()
    columns = df.columns
    df = df.drop(["여자70세이상생활인구수"], axis=1)
    df.columns = columns[1:]

    # 필요한 column만 남기기
    df = df[
        [
            "기준일ID",
            "시간대구분",
            "행정동코드",
            "남자20세부터24세생활인구수",
            "남자25세부터29세생활인구수",
            "남자30세부터34세생활인구수",
            "여자20세부터24세생활인구수",
            "여자25세부터29세생활인구수",
            "여자30세부터34세생활인구수",
        ]
    ]

    # column 이름 바꾸기
    df.columns = [
        "date",
        "time",
        "dong_code",
        "man_20_24_pop",
        "man_25_29_pop",
        "man_30_34_pop",
        "woman_20_24_pop",
        "woman_25_29_pop",
        "woman_30_34_pop",
    ]

    # 행자부 코드를 통계청 코드로 변환
    code_h2s_path = utils.check_path("생활인구", "생활인구_행정동_코드_h2s.json")
    code_h2s = utils.load_data(code_h2s_path)
    df["dong_code"] = df["dong_code"].apply(str)
    df["dong_code"] = df["dong_code"].map(code_h2s)

    # 행정동 코드를 최신 행정동 코드로 변경
    # - (과거) 강남구 일원2동(11230740) → (현재) 강남구 개포3동(11230511)
    # - (과거) 강동구 상일동(11250520) → (현재) 강동구 상일1동(11250760)
    # - (과거) 강동구 강일동(11250510) → (현재) 강동구 강일동(11250750), 강동구 상일2동(11250770)
    # - (과거) 구로구 오류2동(11170680) → (현재) 구로구 오류2동(11170730), 구로구 항동(11170740)
    new_list = []
    for row in df.to_dict("records"):
        if row["dong_code"] == "11230740":
            row["dong_code"] = "11230511"
            new_list.append(row)
        elif row["dong_code"] == "11250520":
            row["dong_code"] = "11250760"
            new_list.append(row)
        elif row["dong_code"] == "11250510":
            row["man_20_24_pop"] = row["man_20_24_pop"] / 2
            row["man_25_29_pop"] = row["man_25_29_pop"] / 2
            row["man_30_34_pop"] = row["man_30_34_pop"] / 2
            row["woman_20_24_pop"] = row["woman_20_24_pop"] / 2
            row["woman_25_29_pop"] = row["woman_25_29_pop"] / 2
            row["woman_30_34_pop"] = row["woman_30_34_pop"] / 2
            row["dong_code"] = "11250750"
            new_list.append(row)
            row_2 = row.copy()
            row_2["dong_code"] = "11250770"
            new_list.append(row_2)
        elif row["dong_code"] == "11170680":
            row["man_20_24_pop"] = row["man_20_24_pop"] / 2
            row["man_25_29_pop"] = row["man_25_29_pop"] / 2
            row["man_30_34_pop"] = row["man_30_34_pop"] / 2
            row["woman_20_24_pop"] = row["woman_20_24_pop"] / 2
            row["woman_25_29_pop"] = row["woman_25_29_pop"] / 2
            row["woman_30_34_pop"] = row["woman_30_34_pop"] / 2
            row["dong_code"] = "11170730"
            new_list.append(row)
            row_2 = row.copy()
            row_2["dong_code"] = "11170740"
            new_list.append(row_2)
        else:
            new_list.append(row)
    df = pd.DataFrame(new_list)

    # 행정동 이름 추가
    name2code_file_path = os.path.join(utils.check_path("행정동"), "*.json")
    name2code_file_list = glob.glob(name2code_file_path)
    name2code_file_path = [f for f in name2code_file_list if "코드" in f][0]
    name2code = utils.load_data(name2code_file_path)

    code2name = {v: k for k, v in name2code.items()}
    df["dong_name"] = df["dong_code"].map(code2name)
    df = df[
        [
            "date",
            "time",
            "dong_name",
            "dong_code",
            "man_20_24_pop",
            "man_25_29_pop",
            "man_30_34_pop",
            "woman_20_24_pop",
            "woman_25_29_pop",
            "woman_30_34_pop",
        ]
    ]

    data_save_path = data_file_path.replace("raw", "clean")
    utils.save_data(data_save_path, df, encoding="cp949")
    LOGGER.info("데이터 처리 완료 - 생활이동 행정동 데이터")


def sum_living_population_by_dong():
    """생활인구 데이터를 이용하여, 어느 자치구에 청년이 가장 많이 머무는지 확인하기"""
    LOGGER.info("=============================================================")
    LOGGER.info("데이터 분석 시작 - [ 청년들이 많이 머무르는 행정동, 자치구 찾기 ]")
    LOGGER.info("=============================================================")

    file_path = os.path.join(utils.check_path("생활인구"), "*.csv")
    file_path_list = glob.glob(file_path)
    file_path = [f for f in file_path_list if "생활인구_데이터" in f and "clean" in f][0]
    df = utils.load_data(file_path, encoding="cp949")

    # 행정동 데이터 분석
    dong_df = df.groupby(["dong_name", "dong_code"]).agg(
        {
            "man_20_24_pop": "sum",
            "man_25_29_pop": "sum",
            "man_30_34_pop": "sum",
            "woman_20_24_pop": "sum",
            "woman_25_29_pop": "sum",
            "woman_30_34_pop": "sum",
        }
    )
    dong_df = dong_df.reset_index(drop=False)
    dong_df["pop_sum"] = dong_df[
        [
            "man_20_24_pop",
            "man_25_29_pop",
            "man_30_34_pop",
            "woman_20_24_pop",
            "woman_25_29_pop",
            "woman_30_34_pop",
        ]
    ].sum(axis=1)
    dong_df = dong_df[["dong_name", "dong_code", "pop_sum"]]
    dong_df_save_path = utils.check_path("생활인구", "생활인구_분석_행정동별_청년_생활인구_합.csv")
    utils.save_data(dong_df_save_path, dong_df, encoding="cp949")
    LOGGER.info("데이터 분석 완료 - 청년들이 많이 머무르는 행정동 찾기")

    # 자치구 데이터 분석
    gu2pop = defaultdict(int)
    for row in dong_df.itertuples():
        gu = row.dong_name.split()
        gu = f"{gu[0]} {gu[1]}"
        gu2pop[gu] += row.pop_sum
    gu_df = pd.DataFrame(gu2pop.items(), columns=["gu_name", "pop_sum"])
    gu_df = gu_df.sort_values(by=["pop_sum"], ascending=False)
    gu_df_save_path = utils.check_path("생활인구", "생활인구_분석_자치구별_청년_생활인구_합.csv")
    utils.save_data(gu_df_save_path, gu_df, encoding="cp949")
    LOGGER.info("데이터 분석 완료 - 청년들이 많이 머무르는 자치구 찾기")


def visualize_living_population_by_dong():
    """행정동별 생활인구 수 시각화하기"""
    LOGGER.info("=============================================================")
    LOGGER.info("데이터 분석 시작 - [ 행정동별 생활인구 수 시각화 ]")
    LOGGER.info("=============================================================")

    df_file_path = utils.check_path("생활인구", "생활인구_분석_행정동별_청년_생활인구_합.csv")
    df = utils.load_data(df_file_path, encoding="cp949")

    # 시각화
    df["dong_code"] = df["dong_code"].apply(str)
    utils_vis.visualize_by_dong(
        df=df,
        columns=["dong_code", "pop_sum"],
        aliases_columns=["행정동:", "생활인구 합:"],
        legend_name="행정동별_생활인구_수_합",
    )


if __name__ == "__main__":
    # process_living_population_dong_code()
    # process_living_population_data()
    # sum_living_population_by_dong()
    visualize_living_population_by_dong()
