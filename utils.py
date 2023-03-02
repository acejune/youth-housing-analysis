import os
import logging
import json
import pandas as pd


DATA_DIR = os.path.join(os.path.abspath('.'), '_dataset')


def check_path(*paths):
    """경로에 사용되는 디렉토리가 존재하는지 확인 후, 없다면 새로 생성하기
    """
    paths = list(paths)
    paths.insert(0, DATA_DIR)
    full_path = os.path.sep.join(paths)

    if '.' in paths[-1]:  # 파일인 경우
        dir_path = os.path.sep.join(paths[:-1])
    else:  # directory인 경우
        dir_path = os.path.sep.join(paths)

    if not os.path.isdir(dir_path):
        os.makedirs(dir_path)
    return full_path


def save_data(file_path, data, encoding='utf-8'):
    """확장자에 따라, data를 file_path에 저장하기
    """
    ext = file_path.split('.')[-1]
    if ext == 'json':
        with open(file_path, 'w', encoding=encoding) as jf:
            json.dump(data, jf, indent='\t', ensure_ascii=False)
    elif ext == 'csv':
        data.to_csv(file_path, encoding=encoding, index=False)


def load_data(file_path, encoding='utf-8'):
    """확장자에 따라, file_path에 저장된 데이터 불러오기
    """
    if not os.path.isfile(file_path):
        raise Exception(f'파일 없음: "{file_path}"')

    ext = file_path.split('.')[-1]
    if ext == 'json':
        with open(file_path, 'r', encoding=encoding) as jf:
            data = json.load(jf)
    elif ext == 'csv':
        data = pd.read_csv(file_path, encoding=encoding)
    elif ext == 'txt':
        with open(file_path, 'r', encoding=encoding) as f:
            data = f.read()
    elif ext == 'xlsx':
        data = pd.read_excel(file_path)
    return data


def set_logger():
    """로그 기록을 위해 로거(logger) 설정하기
    """
    logger = logging.getLogger()
    if len(logger.handlers) > 0:
        return logger
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter("[%(asctime)19s] [%(levelname)8s] [%(filename)25s:%(lineno)4d] %(message)s")

    s_handler = logging.StreamHandler()
    s_handler.setLevel(logging.DEBUG)
    s_handler.setFormatter(formatter)
    logger.addHandler(s_handler)

    save_path = 'main.log'
    f_handler = logging.FileHandler(filename=save_path)
    f_handler.setLevel(logging.DEBUG)
    f_handler.setFormatter(formatter)
    logger.addHandler(f_handler)
    return logger
