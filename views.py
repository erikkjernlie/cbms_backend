import json
import os
import struct
import typing
import os, sys
from aiohttp import web
import aiohttp
import zipfile
from pathlib import PurePath
import numpy as np
import matplotlib.pyplot as plt
import scipy.fftpack
import threading
import pandas as pd
from pandas_profiling import ProfileReport
import codecs
sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)))
from src.views import make_spectrogram
import urllib
from src.datasources.models import generate_catman_outputs, UdpDatasource
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


@routes.get('/project/spectrogram/file/{project_id}/{file_path}/{frequency}', name='spectrogram_from_file')
async def spectrogram_from_file(request: web.Request):
    project_name = request.match_info['project_id']
    file_path = request.match_info['file_path']
    frequency = float(request.match_info['frequency'])
    path = request.app['settings'].PROJECT_DIR + "/" + project_name + '/files/' + file_path

    if ".csv" in file_path:
        data = pd.read_csv(path)
    elif ".xlsx" in file_path:
        data = pd.read_excel(path)
    else:
        print("No file?")

    measurements = data.iloc[:, 1].values
    file_data = make_spectrogram([m for m in measurements], frequency)

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
        print("No file?")
        raise web.HTTPNotFound(reason='No file found')

    if data.columns.values[0] == 'Timestamp':
        fft_values = scipy.fftpack.fft(data.iloc[:, 1].values)
        name = data.columns.values[1]
    else:
        fft_values = scipy.fftpack.fft(data.iloc[:, 0].values)
        name = data.columns.values[0]

    window, cols = data.shape
    step_size = 1.0 / (2.0 * sample_spacing)
    last_arg = int(window / 2)

    x = np.linspace(0.0, step_size, last_arg)
    y = 2.0 / window * np.abs(fft_values[:window // 2])
    plots = [name, [val for val in x], [float(val) for val in y]]

    return web.json_response(plots, dumps=dumps)


@routes.post('/project/new', name='create_project')
async def create_project(request: web.Request):
    r = await request.post()
    data = r['file'] # data is the file

    headers = request.headers
    content_length = int(headers['Content-length'])
    projectName = headers["projectName"]

    os.makedirs(request.app['settings'].PROJECT_DIR + "/" + projectName, exist_ok=True)

    # Write ".FMM" to disc
    if (".fmm" in data.filename):
        # request.app cannot be correct?
        fmmPath = request.app['settings'].PROJECT_DIR + "/" + projectName + "/" + data.filename
        with open(fmmPath, 'w', newline='\n')as file:
            file.write(data.file.read(content_length).decode('ascii')) # writes .fmm to file

    print(request.app['settings'].PROJECT_DIR, projectName, data.filename)

    # Write ".FMU" to disc
    if (".fmu" in data.filename):
        fmuPath = request.app['settings'].PROJECT_DIR + "/" + projectName + "/" + data.filename
        with open(fmuPath, 'wb') as file:
            file.write(data.file.read(content_length)) # writes .fmu to file
        thread = threading.Thread(target=processFMUfile, args=(request, fmuPath))
        #await processFMUfile(request, fmuPath)
        thread.daemon = True  # Daemonize thread
        thread.start()

    return web.HTTPAccepted()


def processFMUfile(request, fmuPath):

    projectName = request.headers["projectName"]
    # Get file name
    fileString = fmuPath.split('/')[-1]
    fileString = fileString.split('.')[0]

    # Open FMU(ZIP) file
    zFile = zipfile.ZipFile(fmuPath)
    fmmContent = zFile.open('resources/model/response.bak.fmm').read().decode('utf-8')

    path = request.app['settings'].PROJECT_DIR + '/' + projectName + '/'

    # CHECK DIRECTORY
    os.makedirs(os.path.dirname(path), exist_ok=True)
    # Write ".FMM" file to disc
    with open(request.app['settings'].PROJECT_DIR + '/' + projectName + '/' + fileString + '.fmm', 'w', newline='\n') as file:
        file.write(fmmContent)

    # Locate all ".FTL" files (All parts)
    for name in zFile.namelist():
        pathCheck = PurePath(name)
        # Check for ".FTL" files in the directory "resources/link_DB"
        if pathCheck.parent.parts == ('resources', 'link_DB') and pathCheck.suffix == '.ftl':
            ftlContent = zFile.open(name)
            ftlFileName = name.split('/')[-1]

            # Write ".FTL" files to disc
            with open(path + ftlFileName, 'w', newline='\n') as file:
                file.write(ftlContent.read().decode('utf-8'))

    os.makedirs(os.path.dirname(request.app['settings'].PROJECT_DIR + fileString + "\\json\\"),
                exist_ok=True)
    fileString = fileString + ".fmm"
    # Generate a JSON Master file for the 3D visualisation
    listOfFiles = createDigitalTwin(fileString, request.app['settings'].PROJECT_DIR + "/" + projectName + "/")
    # Generate JSON files from the ".FTL" files. Used for 3D visualisation
    generatePart(listOfFiles,  request.app['settings'].PROJECT_DIR + "/" + projectName + "/", request.app['settings'].PROJECT_DIR + "/" + projectName + "/json/")


@routes.post('/project/ftlFiles', name='handleFTLfiles')
async def handleFTLfiles(request: web.Request):

    r = await request.post()
    projectName = request.headers["projectName"]
    fileName = request.headers["fileName"]

    files = []
    for x in r:
        data = r[x]
        headers = request.headers
        content_length = int(headers['Content-length'])

        path = request.app['settings'].PROJECT_DIR + "/" + projectName + "/"+ data.filename
        with open(path, 'w', newline='\n')as file:
            # content_length is TOO long here? But it works..
            file.write(data.file.read(content_length).decode('ascii'))
            # add content of each file
            files.append(data.file.read(content_length))

    listOfFiles = createDigitalTwin(fileName + ".fmm", request.app['settings'].PROJECT_DIR + "/" + projectName + "/")
    generatePart(listOfFiles, request.app['settings'].PROJECT_DIR + "/" + projectName + "/", request.app['settings'].PROJECT_DIR + "/" + projectName + "/json/");

    return web.HTTPOk()





