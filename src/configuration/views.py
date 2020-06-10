import json
import os, sys
from aiohttp import web

import numpy as np
import scipy.fftpack
import pandas as pd
from pandas_profiling import ProfileReport
sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)))
from src.topics.views import make_spectrogram
from src.utils import RouteTableDefDocs, dumps, try_get, get_client, try_get_validate, try_get_all
routes = RouteTableDefDocs()


@routes.post('/project/dataset', name='testing')
async def testing(request: web.Request):
    r = await request.post()
    data = r['file'] # data is the file

    headers = request.headers
    content_length = int(headers['Content-length'])
    projectName = "testing"

    os.makedirs(request.app['settings'].PROJECT_DIR + "/" + projectName, exist_ok=True)

    # Write ".FMU" to disc
    if ".csv" in data.filename:
        fmuPath = request.app['settings'].PROJECT_DIR + "/" + projectName + "/" + data.filename
        with open(fmuPath, 'wb') as file:
            file.write(data.file.read(content_length)) # writes .fmu to file
        df = pd.read_csv(request.app['settings'].PROJECT_DIR + "/" + projectName + "/" + data.filename)

        profile = ProfileReport(df, title='Pandas Profiling Report', html={'style': {'full_width': True}})

        profile.to_file(output_file="your_report.html")
        with open("your_report.html", "r", encoding='utf-8') as f:
            text = f.read()
            print(text)
            return web.Response(
            text=text,
            content_type='text/html')

    else:
        return web.HTTPOk()

        #profile.to_file(output_file="your_report.json")


@routes.get('/project/{project}/files/{file}', name='get_datafile')
async def get_datafile(request: web.Request):
    filename = request.match_info['file']
    project = request.match_info['project']

    if ".csv" in filename or ".xlsx" in filename:
        path = request.app['settings'].PROJECT_DIR + "/" + project + "/files/" + filename
        if ".csv" in filename:
            data = pd.read_csv(path)
            header = data.iloc[0]
            window, cols = data.shape
            y = [[val for val in data.values]]
            print("header", header)
            print("head", data.head())
            return web.json_response({
                "values": data.values,
                "y": y,
                "names": list(data)
            }, dumps=dumps)
        elif ".xlsx" in filename:
            data = pd.read_excel(path)
            return web.json_response({
               "values": data.values,
                "names": list(data)
            }, dumps=dumps)


@routes.get('/project/{project}/files/{file}/inspect', name='inspect_data')
async def inspect_data(request: web.Request):
    filename = request.match_info['file']
    project = request.match_info['project']
    df = pd.read_csv(request.app['settings'].PROJECT_DIR + "/" + project + "/files/" + filename)

    profile = ProfileReport(df, title='Pandas Profiling Report', html={'style': {'full_width': True}})
    path = request.app['settings'].PROJECT_DIR + "/" + project + "/files/" + filename.replace(".csv", ".html")
    profile.to_file(output_file=path)
    with open(path, "r", encoding='utf-8') as f:
        text = f.read()
        print("responding now")
        return web.Response(
            text=text,
            content_type='text/html')


@routes.get('/project/datafile/get/{project}/{file}', name='send_historical_data')
async def send_historical_data(request: web.Request):
    file_name = request.match_info['file']
    project_id = request.match_info['project']

    df = []
    if ".csv" in file_name:
        df = pd.read_csv(request.app['settings'].PROJECT_DIR + "/" + project_id + "/files/" + file_name)
    elif ".xlsx" in file_name:
        df = pd.read_excel(request.app['settings'].PROJECT_DIR + "/" + project_id + "/files/" + file_name)
    else:
        web.HTTPBadRequest()

    headers = [title for title in df.columns.values]
    x_axis = [int(timestamp) for timestamp in df.iloc[:, 0].values]
    y_values = [[item for item in df.iloc[:, i].values] for i in range(1, df.shape[1])]
    test = [headers, x_axis, y_values]

    return web.json_response(test, dumps=dumps)


@routes.post('/project/datafile', name='upload_datafile')
async def upload_datafile(request: web.Request):
    r = await request.post()
    data = r['file']

    headers = request.headers
    content_length = int(headers['Content-length'])
    project_name = headers["projectName"]
    os.makedirs(request.app['settings'].PROJECT_DIR + "/" + project_name + "/files", exist_ok=True)

    # Write ".FMU" to disc
    if ".csv" in data.filename or ".xlsx" in data.filename:
        path = request.app['settings'].PROJECT_DIR + "/" + project_name + "/files/" + data.filename
        with open(path, 'wb') as file:
            file.write(data.file.read(content_length))
        return web.HTTPAccepted()
    else:
        return web.HTTPBadRequest()


@routes.post('/machinelearning/{project_id}/{tile}/model', name='save_machinelearning_model')
async def save_machinelearning_model(request: web.Request):
    r = await request.post()
    project_id = request.match_info['project_id']
    tile = request.match_info['tile']
    print(r, project_id, tile)
    data = r['model.json']
    data2 = r['model.weights.bin']

    headers = request.headers
    content_length = int(headers['Content-length'])
    os.makedirs(request.app['settings'].PROJECT_DIR + "/" + project_id + "/" + tile, exist_ok=True)

    try:
        path = request.app['settings'].PROJECT_DIR + "/" + project_id + "/" + tile + "/model.json"
        with open(path, 'wb') as file:
            file.write(data.file.read(content_length))
        path2 = request.app['settings'].PROJECT_DIR + "/" + project_id + "/" + tile + "/models.weights.bin"
        with open(path2, 'wb') as file2:
            file2.write(data2.file.read(content_length))
        return web.HTTPAccepted()
    except:
        return web.HTTPNotFound()


@routes.get('/machinelearning/{project_id}/{tile}/model.json', name='load_machinelearning_model')
async def load_machinelearning_model(request: web.Request):
    project_id = request.match_info['project_id']
    tile = request.match_info['tile']
    with open(request.app['settings'].PROJECT_DIR + "/" + project_id + "/" + tile + "/model.json", 'r') as json_file:
        js = json.load(json_file)
    return web.json_response(js, dumps=dumps)


@routes.get('/machinelearning/{project_id}/{tile}/model.weights.bin', name='load_machinelearning_model_weights')
async def load_machinelearning_model_weights(request: web.Request):
    project_id = request.match_info['project_id']
    tile = request.match_info['tile']
    return web.FileResponse(request.app['settings'].PROJECT_DIR + "/" + project_id + "/" + tile + "/models.weights.bin")



@routes.get('/project/spectrogram/file/{project_id}/{file_path}/{frequency}', name='spectrogram_from_file')
async def spectrogram_from_file(request: web.Request):
    project_name = request.match_info['project_id']
    file_path = request.match_info['file_path']
    frequency = float(request.match_info['frequency'])
    path = request.app['settings'].PROJECT_DIR + "/" + project_name + '/files/' + file_path
    print("path", path)
    if ".csv" in file_path:
        data = pd.read_csv(path)
    elif ".xlsx" in file_path:
        data = pd.read_excel(path)
    else:
        print("No file?")

    measurements = [m for m in data.iloc[:, 1].values]
    duration = len(measurements)/frequency
    print("duration", duration)
    file_data = make_spectrogram(data.iloc[:, 1].values, duration)

    return web.json_response(file_data, dumps=dumps)


@routes.get('/fft/file/{project_id}/{file_path}/{sample_spacing}', name='fetch_fft_from_file')
async def fetch_fft_from_file(request: web.Request):
    project_name = request.match_info['project_id']
    file_path = 'files/' + request.match_info['file_path']
    sample_spacing = float(request.match_info['sample_spacing'])
    path = request.app['settings'].PROJECT_DIR + "/" + project_name + '/' + file_path

    if ".csv" in file_path:
        data = pd.read_csv(path)
    elif ".xlsx" in file_path:
        data = pd.read_excel(path)
    else:
        raise web.HTTPNotFound(reason='No file found')

    if data.columns.values[0] == 'Timestamp':
        fft_values = scipy.fftpack.fft(data.iloc[:, 1].values)
        name = data.columns.values[1]
    else:
        fft_values = scipy.fftpack.fft(data.iloc[:, 0].values)
        name = data.columns.values[0]

    window = data.shape[0] - 1

    x = np.fft.rfftfreq(int(window), d=sample_spacing)
    y = 2.0 / window * np.abs(fft_values[:window // 2])
    plots = [name, [val for val in x], [float(val) for val in y]]

    return web.json_response(plots, dumps=dumps)







