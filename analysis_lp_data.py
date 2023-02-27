import os
import glob
from collections import defaultdict
import pandas as pd
import utils


LOGGER = utils.set_logger()


def which_gu_do_youth_stay():
    """생활인구 데이터를 이용하여, 어느 자치구에 청년이 가장 많이 머무는지 확인하기
    """
    LOGGER.info('=============================================================')
    LOGGER.info('데이터 분석 시작 - [ 청년들이 많이 머무르는 자치구, 행정동 찾기 ]')
    LOGGER.info('=============================================================')

    dir_path = os.path.join(utils.check_path('생활인구'), '*.csv')
    file_path_list = glob.glob(dir_path)
    file_path = [f for f in file_path_list if '생활인구_데이터' in f][0]
    df = utils.load_data(file_path)
    df = df.reset_index()
    cols = df.columns
    df = df.drop(['여자70세이상생활인구수'], axis=1)
    df.columns = cols[1:]

    # 생활인구 수 합계 구하기
    hcode2scode = utils.load_data(utils.check_path('생활인구',
                                                   '생활인구_행정동_코드_행자부2통계청.json'))
    code2pop = defaultdict(int)
    for row in df.itertuples():
        code = hcode2scode[str(row.행정동코드)]  # 행자부 코드 → 통계청 코드
        code2pop[code] += row.남자20세부터24세생활인구수
        code2pop[code] += row.남자25세부터29세생활인구수
        code2pop[code] += row.남자30세부터34세생활인구수
        code2pop[code] += row.여자20세부터24세생활인구수
        code2pop[code] += row.여자25세부터29세생활인구수
        code2pop[code] += row.여자30세부터34세생활인구수
    df = pd.DataFrame(code2pop.items(), columns=['행정동_코드', '생활인구_수_합계'])

    # 행정동 이름 추가
    dong2code = utils.load_data(utils.check_path('생활인구',
                                                 '생활인구_행정동_코드_aligned.json'))
    code2dong = {v: k for k, v in dong2code.items()}
    df['행정동_이름'] = df['행정동_코드'].map(code2dong)

    # 행정동 데이터 저장
    df = df[['행정동_이름', '행정동_코드', '생활인구_수_합계']]
    save_path = utils.check_path('생활인구', '생활인구_분석_행정동별_청년_생활인구.csv')
    utils.save_data(save_path, df)
    LOGGER.info('데이터 분석 완료 - 청년들이 많이 머무르는 행정동 찾기')

    # 자치구 데이터 분석
    gu2pop = defaultdict(int)
    for row in df.itertuples():
        gu = row.행정동_이름.split()
        gu = f'{gu[0]} {gu[1]}'
        gu2pop[gu] += row.생활인구_수_합계
    df = pd.DataFrame(gu2pop.items(), columns=['자치구_이름', '생활인구_수_합계'])
    save_path = utils.check_path('생활인구', '생활인구_분석_자치구별_청년_생활인구.csv')
    utils.save_data(save_path, df)
    LOGGER.info('데이터 분석 완료 - 청년들이 많이 머무르는 자치구 찾기')


if __name__ == '__main__':
    which_gu_do_youth_stay()
