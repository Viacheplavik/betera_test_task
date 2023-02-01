# survey_id - survey object data->id
# interview_status - survey object data->status
# is_test_data - response sub object data->is_test_data
# date_submitted - -//- -> data->date_submitted
# date_started - -//- data->date_started
# uc_key - ? 7 цифр букв
# ip_address - response sub object data->ip_address
# user_agent - -//- data -> user_agent
# response_time - -//-
# country - -//-
# city -//-
# question_id question sub object
# question_name ?
# question_type - question sub object
# is_question_shown - question sub object
# answer - question sub object
# answer_id - question sub object

import yaml
from requests_oauthlib import OAuth1Session
import requests
import json
import csv
import pandas as pd
from typing import List

OUT_FOLDER = 'output_files'

API_LINK = 'http://api.alchemer.eu/v5/'

creds = ['fb95170edd31b27403f83ec50e3832b76c80ba9e54974f8f7b', 'A9kK0bboHyjfw']


class SurveyReporting:
    def __init__(
            self,
            schema='https',
            host='api.alchemer.eu',
            api_version='v5'
    ):
        # self.__api_token, self.__api_secret_token = creds
        self.schema = schema
        self.host = host
        self.api_version = api_version
        self.__params = {
            'api_token': 'fb95170edd31b27403f83ec50e3832b76c80ba9e54974f8f7b',
            'api_token_secret': 'A9kK0bboHyjfw',
            'page': '{0}'
        }

        alchemer = OAuth1Session(
            self.__params['api_token'],
            client_secret=self.__params['api_token_secret']
        )
        url = 'http://api.alchemer.eu/v5/oauth/request_token'
        response = alchemer.get(url)
        response.close()
        self.session = requests.Session()

    def __get_creds(self):
        pass

    def check_page_total(self, url: str):
        # method created to parse page total
        # once for each entity and avoid code dublication
        self.__params['page'] = self.__params['page'].format(1)
        page_total = self.session.get(url, params=self.__params).json()['total_pages']

        return page_total

    @staticmethod
    def json_to_csv(response_data, out_csv_path):
        # change df to dict
        df.to_csv(out_csv_path)

    def merge_pages(self, url) -> List:
        merged_response = list()
        page_total = self.check_page_total(url)
        counter = 1
        while counter != page_total + 1:
            self.__params['page'] = self.__params['page'].format(counter)
            merged_response += self.session.get(url, params=self.__params).json()['data']
            counter += 1
        return merged_response

    @staticmethod
    def data_dict_from_response(response, required_fields):
        # not needed
        data_dict = dict()
        for entity in required_fields:
            data_dict[entity] = list()
        for item in response:
            for field in required_fields:
                data_dict[field].append(item[field])
        return data_dict

    def get_surveys(self):
        print(2222222222222222)
        transformed_surveys = {
            'id': [],
            'team': [],
            'type': [],
            'status': [],
            'created_on': [],
            'modified_on': [],
            'title': [],  # ?
            'statistics': [],

        }
        url = '{schema}://{host}/{api_version}/survey/'.format(
            schema=self.schema,
            host=self.host,
            api_version=self.api_version
        )

        surveys = self.merge_pages(url)
        print(44444444)
        for survey in surveys:
            transformed_surveys['id'].append(survey['id'])
            transformed_surveys['team'].append(survey['team'])
            transformed_surveys['type'].append(survey['type'])
            transformed_surveys['status'].append(survey['status'])
            transformed_surveys['created_on'].append(survey['created_on'])
            transformed_surveys['modified_on'].append(survey['modified_on'])
            transformed_surveys['title'].append(survey['title'])
            transformed_surveys['statistics'].append(survey['statistics'])
        print(33333)
        return transformed_surveys

    def get_user_responses(self):
        print(111111111)
        transformed_user_responses = {
            'id': [],
            'status': [],
            'is_test_data': [],
            'date_submitted': [],
            'session_id': [],
            'language': [],
            'date_started': [],
            'link_id': [],
            'uc_key': [],
            'ip_address': [],
            'referer': [],
            'user_agent': [],
            'response_time': [],
            'longitude': [],
            'latitude': [],
            'country': [],
            'city': [],
            'region': [],
            'postal': [],
            'dma': []
            # add questions info

        }

        transformed_user_answers = {
            'response_id': [],
            'type': [],
            'question': [],
            'section_id': [],  # ?
            'answer': [],
            'answer_id': [],
            'shown': []

        }

        survey_ids = self.get_surveys()['id']
        user_responses = list()

        for survey_id in survey_ids:
            print(survey_id)
            url = '{schema}://{host}/{api_version}/survey/{survey_id}/surveyresponse'.format(
                schema=self.schema,
                host=self.host,
                api_version=self.api_version,
                survey_id=survey_id
            )
            user_responses += self.merge_pages(url)




        for user_response in user_responses:
            transformed_user_responses['id'].append(user_response['id'])
            transformed_user_responses['status'].append(user_response['status'])
            transformed_user_responses['is_test_data'].append(user_response['is_test_data'])
            transformed_user_responses['date_started'].append(user_response['date_started'])
            transformed_user_responses['session_id'].append(user_response['session_id'])
            transformed_user_responses['language'].append(user_response['language'])
            transformed_user_responses['date_started'].append(user_response['date_started'])
            transformed_user_responses['link_id'].append(user_response['link_id'])
            transformed_user_responses['uc_key'].append(user_response['url_variables']['uc']['value'])
            transformed_user_responses['ip_address'].append(user_response['ip_address'])
            transformed_user_responses['referer'].append(user_response['referer'])
            transformed_user_responses['user_agent'].append(user_response['user_agent'])
            transformed_user_responses['response_time'].append(user_response['response_time'])
            transformed_user_responses['longitude'].append(user_response['longitude'])
            transformed_user_responses['latitude'].append(user_response['latitude'])
            transformed_user_responses['country'].append(user_response['country'])
            transformed_user_responses['city'].append(user_response['city'])
            transformed_user_responses['region'].append(user_response['region'])
            transformed_user_responses['postal'].append(user_response['postal'])
            transformed_user_responses['dma'].append(user_response['dma'])

            for answer in user_response['survey_data'].values():
                transformed_user_answers['response_id'].append(answer['id'])
                transformed_user_answers['type'].append(answer['type'])
                transformed_user_responses['question'].append(answer['question'])
                transformed_user_responses['section_id'].append(answer['section_id'])
                transformed_user_responses['answer'].append(answer['answer'])
                transformed_user_responses['answer_id'].append(answer['answer_id'])
                transformed_user_responses['shown'].append(answer['shown'])
        print(777777777777)
        return transformed_user_responses, transformed_user_answers

    def get_statistics(self):
        transformed_statistics = {
            'id': [],
            'base_type': [],
            'type': [],
            'shortname': [],
            'session_id': [],
            'language': [],
            'date_started': [],
            'link_id': [],
            'uc_key': [],
            'ip_address': [],
            'referer': [],
            'user_agent': [],
            'response_time': [],
            'longitude': [],
            'latitude': [],
            'country': [],
            'city': [],
            'region': [],
            'postal': [],
            'dma': []

        }

    def get_questions(self):
        transformed_questions = {

        }




def json_to_file(json_obj, file_name):
    json_object = json.dumps(json_obj, indent=4)
    with open(file_name, "w") as outfile:
        outfile.write(json_object)


def main():
    report = SurveyReporting()




if __name__ == '__main__':
    main()
