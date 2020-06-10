import os, shutil
import firebase_admin
from firebase_admin import firestore
from firebase_admin import credentials
import json

from settings import PROCESSOR_DIR, DATASOURCE_DIR, PROJECT_DIR

current_folder = os.path.dirname(os.path.abspath(__file__))
my_file = os.path.join(current_folder, 'serviceAccountKey.json')
cred = credentials.Certificate(my_file)
firebase_admin.initialize_app(cred)
db = firestore.client()

""" PROFILE """


async def get_user_profile(email):
    doc_ref = db.collection('profiles').document(email)

    try:
        doc = doc_ref.get()
        # print(u'Document data: {}'.format(doc.to_dict()))
        return doc.to_dict()
    except:
        return None


async def create_user_profile(email, first_name, last_name, occupation, phone):
    try:
        db.collection('profiles').document(email).set({
            "firstName": first_name,
            "lastName": last_name,
            "occupation": occupation,
            "projects": [],
            "phone": phone
        })
        return True
    except:
        return False


async def delete_project_from_profile(email, project):
    try:
        project_data = db.collection("projects").document(project).get()
        project_dict = project_data.to_dict()
        users = project_dict["users"]
        print("users")
        if len(users) == 1:
            folder = os.path.join(PROJECT_DIR, project)
            print("folder")
            if os.path.isfile(folder):
                for filename in os.listdir(folder):
                    file_path = os.path.join(folder, filename)
                    try:
                        if os.path.isfile(file_path) or os.path.islink(file_path):
                            os.unlink(file_path)
                        elif os.path.isdir(file_path):
                            shutil.rmtree(file_path)
                    except Exception as e:
                        print('Failed to delete %s. Reason: %s' % (file_path, e))
        print("after deleting")
        ref = db.collection("profiles").document(email)
        ref.update({
            "projects": firestore.ArrayRemove([project])
        })
        # TODO: delete in firestore with subcollections
        return True
    except:
        return False


""" PROJECT """


async def get_project(project):
    try:
        p = {}
        project_data = db.collection("projects").document(project).get().to_dict()
        dashboards = []
        models = []
        dashboards_data = db.collection("projects").document(project).collection("dashboards").stream()
        models_data = db.collection("projects").document(project).collection("models").stream()
        for dashboard_data in dashboards_data:
            dashboards.append(dashboard_data.to_dict())
        for model_data in models_data:
            models.append(model_data.to_dict())
        p["dashboards"] = dashboards
        p["models"] = models
        p["project"] = project_data
        print(project_data, models, dashboards)
        return p
    except:
        return False


async def get_tiles(project, dashboard):
    try:
        tiles = []
        tiles_data = db.collection("projects").document(project).collection("dashboards").document(
            dashboard).collection("tiles").stream()
        for tile_data in tiles_data:
            tile = tile_data.to_dict()
            tile["id"] = tile_data.id
            print("tile", tile)
            tiles.append(tile)
        return tiles
    except:
        return False


async def get_all_projects(project):
    try:
        projects = []
        projects_ref = db.collection(u'projects')

        if project != "none":
            print("project", project)
            query_start_at = projects_ref.order_by(u'name').start_after({
                u'name': project
            }).limit(10)
            docs = query_start_at.stream()
            print("docs", docs)
            for doc in docs:
                print("doc")
                print(doc.to_dict())
                projects.append(doc.to_dict())
        else:
            print("no project")
            query = projects_ref.order_by(u'name').limit(10)
            docs = query.stream()
            for doc in docs:
                print("doc")
                print(doc.to_dict())
                projects.append(doc.to_dict())


        return projects
    except:
        return False


async def get_model(project, model):
    try:
        model_data = db.collection("projects").document(project).collection("models").document(model).get().to_dict()
        return model_data
    except:
        return False


async def get_data_from_projects(projects):
    try:
        docs = db.collection('projects').where("name", "in", projects).stream()
        project_info = []
        for doc in docs:
            project_info.append(doc.to_dict())
        print(project_info)
        return project_info
    except:
        return False





async def create_project(email, project_name, date):
    try:
        print("creating project")
        batch = db.batch()
        print("creating batch")
        project_ref = db.collection("projects").document(project_name)
        print("created ref")
        batch.set(project_ref, {
            "name": project_name,
            "createdAt": date,
            "admins": firestore.ArrayUnion([email]),
            "users": firestore.ArrayUnion([email]),
        })
        print("set project_ref")
        # TODO: to be changed to something else
        user_ref = db.collection("profiles").document(email)
        batch.update(user_ref, {
            "projects": firestore.ArrayUnion([project_name]),
            "project": project_name,
            "lastActive": "date"
        })
        print("added user")
        batch.commit()
        return True
    except:
        return False


async def create_dashboard(project, dashboard):
    try:
        ref = db.collection("projects").document(project).collection("dashboards").document(dashboard)
        ref.set({
            "name": dashboard
        })
        print(project, dashboard)
        return True
    except:
        return False


async def create_model(project, model, filename, timestamp):
    try:
        ref = db.collection("projects").document(project).collection("models").document(model)
        ref.set({
            "name": model,
            "filename": filename,
            "timestamp": timestamp
        })
        return True
    except:
        return False



async def delete_dashboard(project, dashboard):
    try:
        db.collection("projects").document(project).collection("dashboards").document(dashboard).delete()
        return True
    except:
        return False

async def delete_model(project, model):
    try:
        db.collection("projects").document(project).collection("models").document(model).delete()
        return True
    except:
        return False


async def delete_tile(project, dashboard, tile):
    try:
        db.collection("projects").document(project).collection("dashboards").document(dashboard).collection(
            "tiles").document(tile).delete()
        return True
    except:
        return False


async def create_tile(project, dashboard, tile):
    try:
        new_tile = tile.copy()
        print("new tile", new_tile)
        ref = db.collection("projects").document(project).collection("dashboards").document(dashboard).collection(
            "tiles").document()
        new_tile["id"] = ref.id
        print("new_tile_after adding", new_tile)
        ref.set(new_tile)
        return ref.id
    except:
        return ""


async def update(project, dashboard, tile):
    try:
        print("id", tile.id)
        ref = db.collection("projects").document(project).collection("dashboards").document(dashboard).collection(
            "tiles").document(tile.id)
        ref.update(tile)
        return True
    except:
        return False


async def delete_channel(project, dashboard, tile, channel):
    try:
        ref = db.collection("projects").document(project).collection("dashboards").document(dashboard).collection(
            "tiles").document(tile)
        ref.update({
            "channels": firestore.ArrayRemove([channel])
        })
        return True
    except:
        return False


async def add_channel(project, dashboard, tile, channel):
    try:
        ref = db.collection("projects").document(project).collection("dashboards").document(dashboard).collection(
            "tiles").document(tile)
        ref.update({
            "channels": firestore.ArrayUnion([channel])
        })
        return True
    except:
        return False


async def update_tile(project, dashboard, tile):
    try:
        print("id", tile.id)
        ref = db.collection("projects").document(project).collection("dashboards").document(dashboard).collection(
            "tiles").document(tile.id)
        ref.update(tile)
        return True
    except:
        return False


def create_notification(project, notification):
    try:
        ref = db.collection("projects").document(project).collection("notifications").document()
        new_notification = notification.copy()
        new_notification["id"] = ref.id
        ref.set(new_notification)
        return ref.id
    except:
        return False


def update_notification(project, id, notification):
    try:
        ref = db.collection("projects").document(project).collection("notifications").document(id)
        ref.update(notification)
        return True
    except:
        return False


async def get_event_triggers(project):
    try:
        triggers = []
        triggers_data = db.collection("projects").document(project).collection("eventTriggers").stream()
        for trigger_data in triggers_data:
            trigger = trigger_data.to_dict()
            trigger["id"] = trigger_data.id
            print("trigger", trigger)
            triggers.append(trigger)
        return triggers
    except:
        return False


async def delete_event_trigger(project, trigger):
    try:
        db.collection("projects").document(project).collection("eventTriggers").document(trigger).delete()

        return True
    except:
        return False


async def create_event_trigger(project, trigger_id, init_params, topic_id):
    try:
        trigger_ref = db.collection("projects").document(project).collection('eventTriggers').document(trigger_id)
        trigger_ref.set({
            "id": trigger_id,
            "init_params": init_params,
            "topic_id": topic_id,
        })
        return True
    except:
        return False


async def check_project(name):
    return db.collection('projects').document(name).get().exists


async def get_datasources(project_id):
    sources = []
    sources_info = []
    ids = []

    # Get source info from project in firestore
    source_list = db.collection('projects').document(project_id).collection('datasources').stream()
    print("source list from db: ", source_list)
    for source in source_list:
        sources.append(source.to_dict()['name'])
        ids.append(source.to_dict()['id'])
    print("Sources in DB:", sources)
    # Add datasources
    for s in os.listdir(DATASOURCE_DIR):
        if s in sources:
            with open(os.path.join(DATASOURCE_DIR, s), 'r') as f:
                data = json.loads(f.read())
                data['name'] = s
                data['type'] = 'datasource'
                data['id'] = ids[sources.index(s)]
                sources_info.append(json.dumps(data))

    # Get processors
    for s in os.listdir(PROCESSOR_DIR):
        if s[-5:] == '.json' and s[:-5] in sources:
            with open(os.path.join(PROCESSOR_DIR, s), 'r') as f:
                data = json.loads(f.read())
                data['type'] = 'processor'
                data['name'] = data['processor_id']
                data['id'] = ids[sources.index(s[:-5])]
                sources_info.append(json.dumps(data))
    # Return processors that exist in both sources and datasources/processors
    return sources_info


async def add_source(project_id, source_type, source_name):
    existing = db.collection("projects").document(project_id).collection("datasources").stream()
    if source_name in [ex.to_dict()['name'] for ex in existing]:
        return "Name already exists"
    try:
        ref = db.collection("projects").document(project_id).collection("datasources").document()
        new_source = {
            "id": ref.id,
            "type": source_type,
            "name": source_name,
            "project": project_id
        }
        ref.set(new_source)
        return "Success"
    except:
        return "Something went wrong"


async def delete_source(project_id, source_id):
    try:
        db.collection("projects").document(project_id).collection('datasources').document(source_id).delete()
        return True
    except:
        return False
