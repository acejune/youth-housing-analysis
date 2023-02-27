import os
import glob
import utils


LOGGER = utils.set_logger()


def align_lp_dong_code():
    """생활인구 행정동 코드 데이터 정리하기

    NOTE:
        - '최신 행정동 코드'와 '생활인구 행정동 코드'를 비교했을 때, 차이가 있음
        - (과거) 강동구 상일동(11250520) → (현재) 강동구 상일1동(11250760)
        - (과거) 강동구 강일동(11250510) → (현재) 강동구 강일동(11250750), 강동구 상일2동(11250770)
        - (과거) 구로구 오류2동(11170680) → (현재) 구로구 오류2동(11170730), 구로구 항동(11170740)
        - (과거) 강남구 일원2동(11230740) → (현재) 강남구 개포3동(11230511)
    """
    LOGGER.info('=============================================================')
    LOGGER.info('데이터 처리 시작 - [ 생활인구 행정동 코드 ]')
    LOGGER.info('=============================================================')

    file_path = utils.check_path('생활인구', '생활인구_행정동_코드_raw.csv')
    df = utils.load_data(file_path, encoding='cp949')
    df = df.drop([0])

    dong2code = {}
    hcode2scode = {}
    for row in df.itertuples():
        dong = f'{row.시도명} {row.시군구명} {row.행정동명}'
        dong = dong.replace('.', '·')
        s_code = row.통계청행정동코드 + '0'
        h_code = row.행자부행정동코드
        dong2code[dong] = s_code
        hcode2scode[h_code] = s_code

    # 행정동 이름과 (통계청) 행정동 코드 딕셔너리 저장
    dong2code = dict(sorted(dong2code.items(), key=lambda x: x[0]))
    dong2code_save_path = utils.check_path('생활인구',
                                           '생활인구_행정동_코드_aligned.json')
    utils.save_data(dong2code_save_path, dong2code)

    # 생활인구 데이터에서 사용하는 (행자부) 행정동 코드와 (통계청) 행정동 코드 딕셔너리 저장
    hcode2scode = dict(sorted(hcode2scode.items(), key=lambda x: x[0]))
    hcode2scode_save_path = utils.check_path('생활인구',
                                             '생활인구_행정동_코드_행자부2통계청.json')
    utils.save_data(hcode2scode_save_path, hcode2scode)
    LOGGER.info('데이터 처리 완료 - 생활인구 행정동 코드 데이터')

    # # 최신 행정동 코드와 '생활인구'에서 사용하는 (통계청) 행정동 코드 비교
    # latest_dong2code_path = os.path.join(utils.check_path('행정동'), '행정동_코드_*')
    # latest_dong2code_path = glob.glob(latest_dong2code_path)[0]
    # latest_dong2code = utils.load_data(latest_dong2code_path)

    # set_latest_dong2code = set([(k, v) for k, v in latest_dong2code.items()])
    # set_lp_dong2code = set([(k, v) for k, v in dong2code.items()])
    # LOGGER.info(f'(최신 행정동) - (생활인구 행정동) = {set_latest_dong2code - set_lp_dong2code}')
    # LOGGER.info(f'(생활인구 행정동) - (최신 행정동) = {set_lp_dong2code - set_latest_dong2code}')


if __name__ == '__main__':
    align_lp_dong_code()
