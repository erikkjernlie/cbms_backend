from aiohttp import web
from src.utils import RouteTableDefDocs, dumps
import src.database.models as database
import os
import shutil
from src.utils import dumps, RouteTableDefDocs, try_get_all, try_get_validate, try_get, get_client, try_get_topic
import json
from datetime import datetime

routes = RouteTableDefDocs()


@routes.post('/profile/new', name='new_profile')
async def new_profile(request: web.Request):
    print("new_profile")
    post = await request.post()
    email = try_get(post, 'email')
    first_name = try_get(post, 'firstName')
    last_name = try_get(post, 'lastName')
    occupation = try_get(post, 'occupation')
    phone = try_get(post, 'phoneNumber')
    created = await database.create_user_profile(email, first_name, last_name, occupation, phone)
    if created:
        print("created")
        return web.HTTPCreated()
    else:
        return web.HTTPForbidden


@routes.get('/profile/{id}', name='get_profile')
async def get_profile(request: web.Request):
    email = request.match_info['id']
    profile = await database.get_user_profile(email)
    if profile is None:
        raise web.HTTPNotFound()
    return web.json_response(profile, dumps=dumps)


@routes.get('/profile/{email}/projects/{project}/delete', name='delete_project')
async def delete_project(request: web.Request):
    # print("get_profile")
    print("start to delete project")
    email = request.match_info['email']
    project = request.match_info['project']
    print(email, project)
    # profile = await database.get_user_profile(email)
    deleted = await database.delete_project_from_profile(email, project)
    if deleted:
        print("jyust deleted")
        raise web.HTTPOk
    else:
        raise web.HTTPBadRequest


# this method can be changed to a get request and send projects list as params
@routes.get('/projects/all/{project}', name='get_all_projects')
async def get_all_projects(request: web.Request):
    print("new_profile")
    project = request.match_info['project']
    projects = await database.get_all_projects(project)
    if projects is False:
        raise web.HTTPNotFound(reason=f'No data')
    return web.json_response(projects, dumps=dumps)


# this method can be changed to a get request and send projects list as params
@routes.post('/projects/', name='get_projects')
async def get_projects(request: web.Request):
    print("new_profile")
    post = await request.post()
    projects = await try_get_all(post, 'project', str)
    print(projects)
    projects_data = await database.get_data_from_projects(projects)
    if projects_data is False:
        raise web.HTTPServiceUnavailable(reason=f'You can have maximum 10 projects')
    return web.json_response(projects_data, dumps=dumps)


@routes.get('/project/{id}', name='get_project')
async def get_project(request: web.Request):
    id = request.match_info['id']
    project_data = await database.get_project(id)
    if project_data is False:
        raise web.HTTPNotFound(reason=f'No data')
    return web.json_response(project_data, dumps=dumps)


@routes.get('/project/{project}/dashboards/{dashboard}', name='get_tiles')
async def get_tiles(request: web.Request):
    project = request.match_info['project']
    dashboard = request.match_info['dashboard']
    tiles = await database.get_tiles(project, dashboard)
    if tiles is False:
        raise web.HTTPNotFound(reason=f'No data')
    return web.json_response(tiles, dumps=dumps)


@routes.get('/project/{project}/models/{model}', name='get_model')
async def get_model(request: web.Request):
    project = request.match_info['project']
    model = request.match_info['model']
    model_information = await database.get_model(project, model)
    if model_information is False:
        raise web.HTTPNotFound(reason=f'No data')
    return web.json_response(model_information, dumps=dumps)


@routes.post('/projects/new', name='create_new_project')
async def create_new_project(request: web.Request):
    post = await request.post()
    name = try_get(post, 'projectName', str)
    exists = await database.check_project(name)
    if exists:
        raise web.HTTPBadRequest(text='Project name already exists')
    date = try_get(post, 'date', str)
    email = try_get(post, 'email', str)
    created = await database.create_project(email, name, date)
    if created:
        return web.HTTPCreated()
    print("created")
    raise web.HTTPBadRequest()



@routes.post('/projects/{project}/dashboards/new', name='create_dashboard')
async def create_dashboard(request: web.Request):
    post = await request.post()
    project = request.match_info['project']
    dashboard = try_get(post, 'dashboard', str)
    created = await database.create_dashboard(project, dashboard)
    if not created:
        raise web.HTTPBadRequest()
    print("created")
    return web.HTTPCreated()


@routes.post('/projects/{project}/models/new', name='create_model')
async def create_model(request: web.Request):
    post = await request.post()
    project = request.match_info['project']
    model = try_get(post, 'model', str)
    filename = try_get(post, 'filename', str)
    timestamp = datetime.now()
    created = await database.create_model(project, model, filename, timestamp)
    if not created:
        raise web.HTTPBadRequest()
    print("created")
    return web.HTTPCreated()


@routes.get('/projects/{project}/dashboards/{dashboard}/delete', name='delete_dashboard')
async def delete_dashboard(request: web.Request):
    project = request.match_info['project']
    dashboard = request.match_info['dashboard']
    deleted = await database.delete_dashboard(project, dashboard)
    if deleted:
        raise web.HTTPOk
    else:
        raise web.HTTPNotFound(reason=f'Could not find project or dashboard')


@routes.get('/projects/{project}/models/{model}/delete', name='delete_model')
async def delete_model(request: web.Request):
    project = request.match_info['project']
    model = request.match_info['model']
    deleted = await database.delete_model(project, model)
    if deleted:
        raise web.HTTPOk
    else:
        raise web.HTTPNotFound(reason=f'Could not find project or model')


@routes.get('/projects/{project}/dashboards/{dashboard}/tiles/{tile}/delete', name='delete_tile')
async def delete_tile(request: web.Request):
    project = request.match_info['project']
    dashboard = request.match_info['dashboard']
    tile = request.match_info['tile']
    deleted = await database.delete_tile(project, dashboard, tile)
    if deleted:
        raise web.HTTPOk
    else:
        raise web.HTTPNotFound(reason=f'Could not find project, dashboard or tile')


@routes.post('/projects/{project}/dashboards/{dashboard}/tile/new', name='create_tile')
async def create_tile(request: web.Request):
    project = request.match_info['project']
    dashboard = request.match_info['dashboard']
    tile = await request.json()
    created = await database.create_tile(project, dashboard, tile)
    if len(created) == 0:
        raise web.HTTPBadRequest()
    return web.json_response({"id": created}, dumps=dumps)


@routes.post('/projects/{project}/dashboards/{dashboard}/tiles/{id}/update', name='update_tile')
async def update_tile(request: web.Request):
    project = request.match_info['project']
    dashboard = request.match_info['dashboard']
    tile = await request.json()
    updated = await database.update_tile(project, dashboard, tile)
    if updated:
        raise web.HTTPBadRequest()
    return web.HTTPOk()


@routes.post('/projects/{project}/dashboards/{dashboard}/tiles/{tile}/channels/delete', name='delete_channel')
async def delete_channel(request: web.Request):
    post = await request.post()
    project = request.match_info['project']
    dashboard = request.match_info['dashboard']
    tile = request.match_info['tile']
    channel = try_get(post, 'channel', str)
    deleted = await database.delete_channel(project, dashboard, tile, channel)
    if deleted:
        raise web.HTTPOk
    else:
        raise web.HTTPNotFound(reason=f'Could not find project, dashboard, tile or channel')


@routes.post('/projects/{project}/dashboards/{dashboard}/tiles/{tile}/channels/new', name='add_channel')
async def add_channel(request: web.Request):
    post = await request.post()
    project = request.match_info['project']
    dashboard = request.match_info['dashboard']
    tile = request.match_info['tile']
    channel = try_get(post, 'channel', str)
    created = await database.add_channel(project, dashboard, tile, channel)
    print("created", created)
    if not created:
        raise web.HTTPBadRequest()
    return web.HTTPCreated()


@routes.post('/projects/{project}/event_triggers/new', name='create_event_trigger')
async def create_event_trigger(request: web.Request):
    post = await request.post()
    project = request.match_info['project']
    trigger_id = try_get(post, 'id', str)
    init_params = try_get(post, 'init_params', str)
    topic_id = try_get(post, 'topic_id', str)
    created = await database.create_event_trigger(project, trigger_id, init_params, topic_id)
    if not created:
        raise web.HTTPBadRequest()
    print("created")
    return web.HTTPCreated()


@routes.get('/projects/{project}/event_triggers/{trigger}/delete', name='delete_event_trigger')
async def delete_event_trigger(request: web.Request):
    project = request.match_info['project']
    trigger = request.match_info['trigger']
    if trigger in request.app['processors']:
        await request.app['processors'][trigger].stop()
    if trigger not in os.listdir(request.app['settings'].PROCESSOR_DIR):
        raise web.HTTPNotFound()
    shutil.rmtree(os.path.join(request.app['settings'].PROCESSOR_DIR, trigger))
    os.remove(os.path.join(request.app['settings'].PROCESSOR_DIR, trigger + '.json'))

    deleted = await database.delete_event_trigger(project, trigger)
    if deleted:
        raise web.HTTPOk
    else:
        raise web.HTTPNotFound(reason=f'Could not find trigger in project')


@routes.get('/projects/{project}/event_triggers/list', name='event_triggers_list')
async def event_triggers_list(request: web.Request):
    project = request.match_info['project']
    triggers = await database.get_event_triggers(project)
    if triggers is False:
        raise web.HTTPNotFound(reason=f'No data')
    return web.json_response(triggers, dumps=dumps)


@routes.get('/sources/get/{project}', name='get_project_sources')
async def get_project_sources(request: web.Request):
    project_id = request.match_info['project']

    running_sources = request.app['datasources'].get_sources().keys()
    ss = {
        s: s in running_sources
        for s in os.listdir(request.app['settings'].DATASOURCE_DIR)
    }
    topics = request.app['topics']
    topic_name_list = [topics[s]['url'].split("/")[2] for s in topics]
    topic_id_list = [s for s in topics]

    sources = await database.get_datasources(project_id)

    new_sources = []
    for source in sources:
        source = json.loads(source)
        if source['name'] in running_sources:
            source['running'] = ss[source['name']]
        if source['name'] in topic_name_list:
            source['topic'] = topic_id_list[topic_name_list.index(source['name'])]
        elif source['name'] not in topic_name_list:
            source['topic'] = None
            source['started'] = False
            source['initialized'] = False
        new_sources.append(json.dumps(source))

    return web.json_response(new_sources, dumps=dumps)


@routes.get('/sources/{project}/{type}/{name}/new', name='add_source_to_database')
async def add_source_to_database(request: web.Request):
    project_id = request.match_info['project']
    source_type = request.match_info['type']
    source_name = request.match_info['name']
    added = await database.add_source(project_id, source_type, source_name)

    if added == "Success":
        return web.HTTPOk()
    else:
        raise web.HTTPNotFound(reason=added)


@routes.get('/sources/{project}/{id}/delete', name='remove_source_from_database')
async def remove_source_from_database(request: web.Request):
    project_id = request.match_info['project']
    source_id = request.match_info['id']

    deleted = await database.delete_source(project_id, source_id)

    if deleted:
        return web.HTTPOk()
    else:
        raise web.HTTPNotFound(reason=f'Source not deleted')


