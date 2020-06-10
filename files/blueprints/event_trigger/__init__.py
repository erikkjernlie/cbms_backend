"""
A blueprint for event triggering
"""
import aiohttp
from collections import namedtuple
from typing import List
import json
import datetime
import src.database.models as database
import pytz
import multiprocessing
import threading
Variable = namedtuple(
    'Variable',
    ('name', 'valueReference', 'value'),
    defaults=(None, None, 0)
)



class P:
    """The interface for the event trigger functionality"""

    def __init__(self, number_of_inputs, trigger_inputs, project_id, phone_number):
        """"""
        trigger_inputs = json.loads(trigger_inputs)
        print("phone_number", phone_number)
        self.project_id = project_id
        # for i in range(len(trigger_inputs)):
        #    print(trigger_inputs[i])
        self.trigger_inputs = trigger_inputs
        self.phone_number = phone_number
        self.inputs = [
            Variable(
                name=trigger_inputs[i]['name'],
                valueReference=i,
                value=0
            )
            for i in trigger_inputs
        ]
        self.outputs = [
            Variable(
                name=trigger_inputs[i]['name'],
                valueReference=i,
                value=0
            )
            for i in trigger_inputs
        ]

        self.triggered = False
        self.trigger = {}
        self.timeSinceAlert = datetime.datetime.now(pytz.timezone('Europe/Oslo'))

    async def send_sms(self, phone_number):
        print("phone_number send", phone_number)

    def set_inputs(self, input_refs, input_values):
        # print(input_refs)
        # print(input_values)
        # PSEUDO CODE FOR TRIGGER FUNCTIONALITY:
        # print("[event_trigger]: input refs and values", input_refs, input_values)
        for key, val in self.trigger_inputs.items():
            # print("input key", key, "input val: ", val)
            minVal = float(val['min'])
            maxVal = float(val['max'])
            sensor = val['name']

            minSeverityLevel = val['minSeverityLevel']
            maxSeverityLevel = val['maxSeverityLevel']
            minDescription = val['minDescription']
            maxDescription = val['maxDescription']

            index = input_refs.index(key)
            value = input_values[index]
            print("input_values", input_values)
            # print("KEY", key, "input refs", input_refs, "int(key) in input_refs", int(key) in input_refs)
            if key in input_refs:
                # print("KEY", key, "MINVAL",minVal, "VALUE", value, value < minVal)
                # print(value > minVal, (sensor in self.trigger), hasattr(self.trigger[sensor], 'startedAt'), hasattr(self.trigger[sensor], 'trigger_reason'), self.trigger[sensor]["trigger_reason"] == "lt")

                if (value > minVal and (sensor in self.trigger) and ("startedAt" in self.trigger[sensor]) and (
                        self.trigger[sensor]["trigger_reason"] == "lt")):
                    # report to firebase that it is over
                    newId = self.trigger[sensor]["id"]
                    print(newId)
                    updated = database.update_notification(self.project_id, newId, {
                        u'finished': True,
                        u'endedAt': datetime.datetime.now(pytz.timezone('Europe/Oslo')),
                        u'valueExceeded': minVal
                    })

                    del self.trigger[sensor]

                if minVal != None and value < minVal:
                    # we are below minimum
                    if (sensor not in self.trigger and (
                            datetime.datetime.now(pytz.timezone('Europe/Oslo')) - self.timeSinceAlert).seconds > 2):
                        print("we are below, trigger alert")

                        self.timeSinceAlert = datetime.datetime.now(pytz.timezone('Europe/Oslo'))

                        id = database.create_notification(self.project_id, {
                            u'sensor': sensor,
                            u'startedAt': datetime.datetime.now(pytz.timezone('Europe/Oslo')),
                            u'triggerReason': "below",
                            u'finished': False,
                            u"severity": minSeverityLevel,
                            u"description": minDescription,
                        })
                        if id is not False:
                            print("Setting to", id)
                            self.trigger[sensor] = {
                                "startedAt": datetime.datetime.now(pytz.timezone('Europe/Oslo')),
                                "triggered": False,
                                "trigger_reason": "lt",
                                "id": id,

                            }
                if (value < maxVal and (sensor in self.trigger) and ("startedAt" in self.trigger[sensor]) and (
                        self.trigger[sensor]["trigger_reason"] == "gt")):
                    # report to firebase that it is over
                    newId = self.trigger[sensor]["id"]
                    print(newId)
                    updated = database.update_notification(self.project_id, newId, {
                        u'finished': True,
                        u'endedAt': datetime.datetime.now(pytz.timezone('Europe/Oslo')),
                        u'valueExceeded': maxVal
                    })
                    del self.trigger[sensor]
                if maxVal != None and value > maxVal:
                    # we are below minimum
                    if (sensor not in self.trigger and (
                            (datetime.datetime.now(pytz.timezone('Europe/Oslo')) - self.timeSinceAlert).seconds > 2)):

                        self.timeSinceAlert = datetime.datetime.now(pytz.timezone('Europe/Oslo'))

                        id = database.create_notification(self.project_id, {
                            u'sensor': sensor,
                            u'startedAt': datetime.datetime.now(pytz.timezone('Europe/Oslo')),
                            u'triggerReason': "above",
                            u'finished': False,
                            u"severity": maxSeverityLevel,
                            u"description": maxDescription
                        })
                        """
                        TODO: implement SMS
                        if self.phone_number:
                            session = aiohttp.ClientSession()
                            print("self.p", self.phone_number)
                            multiprocessing.Process(target=self.send_sms, args=(self.phone_number,)).start()
                            #session.get('https://sveve.no/SMS/SendMessage?user=USERNAME&passwd=PASSWORD&to=90909090&msg=Hei.%20Du%20har%20f%C3%A5tt%20en%20varsel%20grunnet%20for%20h%C3%B8y%20last%20p%C3%A5%20riggen.%20G%C3%A5%20www.digtwin.surge.sh%20for%20mer%20informasjon.&from=CBMS')
                        """
                        if id is not False:
                            self.trigger[sensor] = {
                            "startedAt": datetime.datetime.now(pytz.timezone('Europe/Oslo')),
                            "triggered": False,
                            "trigger_reason": "gt",
                            "id": id
                        }

    def get_outputs(self, output_refs: List[int]):
        if not self.triggered:
            return None
        print("[self.inputs[ref].value for ref in output_refs]", [self.inputs[ref].valueReference for ref in output_refs])
        return [self.inputs[ref].valueReference for ref in output_refs]

    def step(self, t):
        pass
