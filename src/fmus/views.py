import os

from aiohttp import web
from fmpy import read_model_description
from src.utils import dumps, find_in_dir, RouteTableDefDocs
import zipfile
from pathlib import PurePath
routes = RouteTableDefDocs()
import src.database.models as database
import pytz
from datetime import datetime
import multiprocessing


@routes.get('/models/', name='models')
async def models(request: web.Request):
    """List available models for the fedem blueprint"""
    return web.json_response(os.listdir(request.app['settings'].MODEL_DIR))


@routes.get('/fmus/', name='fmu_list')
async def fmu_list(request: web.Request):
    """
    List all uploaded FMUs.

    Append an FMU id to get more information about a listed FMU.
    """

    return web.json_response(os.listdir(request.app['settings'].FMU_DIR))


@routes.get('/fmus/{id}', name='fmu_detail')
async def fmu_detail(request: web.Request):
    """
    Get detailed information for the FMU with the given id

    Append /models to get the 3d models if any
    """
    file = await find_in_dir(request.match_info['id'], request.app['settings'].FMU_DIR)
    model_description = read_model_description(file)
    return web.json_response(model_description, dumps=dumps)


# @routes.get('/fmus/{id}/vars', name='fmu_vars')
# async def fmu_vars(request: web.Request):
#     """Get sorted variables for the FMU with the given id"""
#     file = await find_in_dir(request.match_info['id'], request.app['settings'].FMU_DIR)
#     model_variables = simulation.ModelVariables(read_model_description(file))
#     return web.json_response(model_variables, dumps=dumps)


@routes.get('/fmus/{id}/models/', name='fmu_models')
async def fmu_models(request: web.Request):
    """
    List the 3d models belonging to the FMU if any exists

    Append the models id the get a specific model
    """
    fmu_id = request.match_info['id']
    fmu_model_root_dir = request.app['settings'].FMU_MODEL_DIR
    models = os.listdir(get_fmu_models_folder(fmu_id, fmu_model_root_dir))
    return web.json_response(models)


@routes.post('/models/upload', name='upload_model')
async def upload_model(request: web.Request):
    r = await request.post()
    data = r['file']  # data is the file

    headers = request.headers
    content_length = int(headers['Content-length'])
    projectName = headers["projectName"]

    os.makedirs(request.app['settings'].PROJECT_DIR + "/" + projectName, exist_ok=True)

    path = request.app['settings'].PROJECT_DIR + "/" + projectName + "/" + data.filename

    print(request.app['settings'].PROJECT_DIR, projectName, data.filename)
    # Write ".FMM" to disc

    #files/projects/project/filename

    if ".fmm" in data.filename:
        with open(path, 'w', newline='\n')as file:
            file.write(data.file.read(content_length).decode('ascii'))  # writes .fmm to file
    else:
        # Write ".FMU" to disc
        data_stream = data.file.read(content_length)
        multiprocessing.Process(target=processFMUfile, args=(path, data_stream, content_length)).start()

    return web.HTTPAccepted()


def processFMUfile(fmuPath, data_stream, content_length):
    # process fmu functionality is not available due to NDA restrictions
    pass


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

    print("filename: ", fileName, "path",  request.app['settings'].PROJECT_DIR + "/" + projectName + "/")
    listOfFiles = createDigitalTwin(fileName + ".fmm", request.app['settings'].PROJECT_DIR + "/" + projectName + "/")
    print("file list", listOfFiles)
    generatePart(listOfFiles, request.app['settings'].PROJECT_DIR + "/" + projectName + "/", request.app['settings'].PROJECT_DIR + "/" + projectName + "/json/");

    return web.HTTPOk()


def get_fmu_models_folder(fmu_id, fmu_model_dir):
    if fmu_id in os.listdir(fmu_model_dir):
        model_folder = os.path.join(fmu_model_dir, fmu_id)
        return model_folder
    else:
        raise web.HTTPNotFound()


@routes.get('/fmus/{id}/models/{model}', name='fmu_model')
async def fmu_model(request: web.Request):
    """Get a 3d model belonging to the FMU if it exists"""
    fmu_id = request.match_info['id']
    fmu_model_root_dir = request.app['settings'].FMU_MODEL_DIR
    model_dir = get_fmu_models_folder(fmu_id, fmu_model_root_dir)
    model = request.match_info['model']
    if model not in os.listdir(model_dir):
        raise web.HTTPNotFound()
    return web.FileResponse(path=os.path.join(model_dir, model))


