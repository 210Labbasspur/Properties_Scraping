#############           EPlace_Sale

import random
import mimetypes
import re
from copy import deepcopy

from botocore.exceptions import NoCredentialsError, ClientError
import requests, json, time, threading, queue, os
import boto3
import pymongo
from datetime import datetime
from botocore.exceptions import NoCredentialsError
import scrapy


class EPlace_Sale(scrapy.Spider):
    name = "EPlace_Sale"

    url = 'https://eplacelive-v2.search.windows.net/indexes/eplacelive-indexv5/docs/search?api-version=2016-09-01'
    prefix = "https://www.eplace.com.au"
    headers = {
  'accept': '*/*',
  'accept-language': 'en-US,en;q=0.9',
  'api-key': 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
  'content-type': 'application/json',
  'origin': 'https://www.eplace.com.au',
  'priority': 'u=1, i',
  'referer': 'https://www.eplace.com.au/sold/property-search',
  'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
  'sec-ch-ua-mobile': '?0',
  'sec-ch-ua-platform': '"Windows"',
  'sec-fetch-dest': 'empty',
  'sec-fetch-mode': 'cors',
  'sec-fetch-site': 'cross-site',
  'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
}
    data = {
            'filter': "listing_sale_or_rental eq 'Sale' and system_listing_state eq 'current' and feed_status ne 'stopped' and tags/all(t: t ne 'LaunchingSoon') and tags/all(t: t ne 'eplace-onhold') and tags/all(t: t ne 'Coming2023') and tags/all(t: t ne 'coming2024') and is_deleted eq false",
            'orderby': 'system_ctime desc',
            'top': 24,
            'skip': 24,
            'count': True,
        }

    count = 1
    db = 'EPlace'
    collection = 'EPlace_Sale'
    # bucket_prefix = f'LRE_{collection}'
    bucket_prefix = f'Images3'

    local_file_path = '/img'

    def __init__(self, *args, **kwargs):
        super(EPlace_Sale, self).__init__(*args, **kwargs)
        self.links = []
        self.database_sale_matching_url = self.read_data_base()


    '''    SCRPAY SCRIPT START'S FROM HERE     '''
    def start_requests(self):
        offset = 0
        payload = deepcopy(self.data)
        payload['skip'] = str(offset)
        yield scrapy.Request(self.url, method='POST',body=json.dumps(payload), headers=self.headers, callback=self.parse,
                             meta={'offset':offset})

    def parse(self, response):
        '''    FARHAN'S LOGIC    '''
        data = json.loads(response.text)
        all_links = []
        links = []
        for each_url in data.get('value',[]):
            each_url = f'https://www.eplace.com.au/properties?external_id={each_url.get("default_external_id", "")}'
            links.append(each_url)
            self.links.append(each_url)

        for each_db_detail_page_url in self.database_sale_matching_url:
            if each_db_detail_page_url in links:
                new_data = {
                    "Field102": "ACTIVE",
                    "Field104": each_db_detail_page_url,
                    "Field3": datetime.now().strftime("%d %b, %Y")
                }
                self.update_database(each_db_detail_page_url, new_data, 'true')
                index_to_remove = links.index(each_db_detail_page_url)
                links.pop(index_to_remove)

            elif each_db_detail_page_url not in self.links:
                new_data = {
                    "Field102": "REMOVED",
                    "Field104": each_db_detail_page_url,
                    "Field3": datetime.now().strftime("%d %b, %Y")
                }
                self.update_database(each_db_detail_page_url, new_data, 'false')


        offset = response.meta['offset']
        for property in data.get('value',[]):
            offset += 1
            property_url = f'https://www.eplace.com.au/properties?external_id={property.get("default_external_id", "")}'
            print(self.count,' # Property URL :', property_url)
            self.count += 1
            yield response.follow(property_url, headers=self.headers, callback=self.detail_parse, meta={'property':property})

        total_records = data.get('@odata.count')
        print(f"Current-Records = {offset} || Total-Records = {total_records}")
        if offset < total_records:
            payload = deepcopy(self.data)
            payload['skip'] = str(offset)
            yield scrapy.Request(self.url, method='POST', body=json.dumps(payload), headers=self.headers, callback=self.parse,
                                 meta={'offset': offset})


    def detail_parse(self, response):
        property = response.meta['property']
        detail_page_url = response.url
        database_sale_matching_url = self.read_data_base()
        if detail_page_url not in database_sale_matching_url:
            item = dict()
            item['Field1'] = ''
            item['Field2'] = int(2217)
            item['Field3'] = datetime.now().date().strftime('%Y-%m-%d')  # date_data_scanned
            item['Field4'] = 'PLACE'

            address = property.get('address_formats_full_address','')   # full_adddress
            postcode = property.get('xxxxxxxx','')   # full_adddress
            item['Field5'] = f"{address} {postcode}"   # full_adddress

            item['Field6'] = property.get('attributes_bedrooms','')  # bedrooms
            item['Field7'] = property.get('attributes_bathrooms','')  # bathrooms
            item['Field8'] = property.get('attributes_garages','') if property.get('attributes_garages','') else property.get('attributes_carports','')

            item['Field9'] = property.get('price_match','')  # price
            item['Field12'] = property.get('advert_internet_body','').replace('\n','').replace('\t','').replace('\r','')  # price

            '''         Uploading Images on Wasabi S3 Bucket            '''
            prop_images = []
            for image in property.get("images",[]):
                image_json = json.loads(image)
                image_url = f"https:{image_json.get('url','')}"
                prop_images.append(image_url)
            images = []
            for img in prop_images:
                img = img.replace('background-image:url(','').replace(");","")
                images.append(f'{img}' if img else "")
            images_string = ', '.join(images)
            # item['Field13'] = images_string
            # print('Property images are :', images_string)
            random_number = random.randint(1, 10000000000)
            item['Field13'] = ", ".join(self.uploaded(images_string, random_number))


            item['Field14'] = response.url  # external_link
            '''          AGENTS          '''
            if property.get("listing_agent_1_name"):
                agent1_name = property.get('listing_agent_1_name','')
                if agent1_name:
                    first_name1, last_name1 = agent1_name.split(maxsplit=1)
                    item['Field15'] = first_name1.strip()  # agent_first_name_1
                    item['Field16'] = last_name1.strip()  # agent_surname_name_1
                    item['Field17'] = agent1_name.strip()  # agent_full_name_1
                    item['Field19'] = property.get('listing_agent_1_email_address','')
                    item['Field20'] = property.get('listing_agent_1_phone_mobile','')
                    item['Field21'] = property.get('listing_agent_1_phone_direct','')
                    '''         Uploading Images on Wasabi S3 Bucket            '''
                    agent_image = property.get('listing_agent_1_profile_image','')
                    images_string = f'{agent_image}' if agent_image else ""
                    item['Field22'] = images_string
                    print('Agent1 image is :',images_string)
                    # random_number = random.randint(1, 10000000000)
                    # item['Field22'] = ",".join(self.uploaded(images_string, random_number))

            if property.get("listing_agent_2_name"):
                agent2 = property.get('listing_agent_2_name', '')
                agent2_name = property.get('listing_agent_2_name', '')
                if agent2:
                    item['Field23'] = agent2_name.strip()
                    item['Field25'] = property.get('listing_agent_2_email_address','')
                    item['Field26'] = property.get('listing_agent_2_phone_mobile','')
                    item['Field26A'] = property.get('listing_agent_2_phone_direct','')
                    '''         Uploading Images on Wasabi S3 Bucket            '''
                    agent_image = property.get('listing_agent_1_profile_image','')
                    images_string = f'{agent_image}' if agent_image else ""
                    item['Field27'] = images_string
                    print('Agent2 image is :',images_string)
                    # random_number = random.randint(1, 10000000000)
                    # item['Field27'] = ",".join(self.uploaded(images_string, random_number))


            item['Field33'] = property.get('system_ctime','')
            item['Field35'] = ' '.join(property.get('subcategories'))
            item['Field36'] = property.get('attributes_landarea_m2','')
            # item['Field37'] = property.get('floor_area','')

            feature_count = 58
            for feature in property.get("features",[]):   ##  first 4 features are ID, bed, bath etc.
                if 'condition' in feature.lower():
                    item['Field52'] = feature
                else:
                    item[f'Field{feature_count}'] = feature
                    feature_count += 1

            ####        ACTIVE / REMOVED Logic implemented in "Field102"
            item['Field102'] = 'ACTIVE'
            item['Field104'] = response.url
            print(item)
            self.insert_database(item)
        else:
            print(f'Skipping Record {detail_page_url}, since it already exists in the Data Base  !!!!! ')


    def read_data_base(self):
    # def read_data_base(self, profileUrl):
        connection_string = 'mongodb://localhost:27017'
        conn = pymongo.MongoClient(connection_string)
        db = conn[self.db]
        collection = db[self.collection]

        sale_urls_list_of_DB = []
        all_matching_data = collection.find()
        for each_row in all_matching_data:
            sale = each_row.get('Field104')
            sale_urls_list_of_DB.append(sale)
        return sale_urls_list_of_DB


    def insert_database(self, new_data):
        connection_string = 'mongodb://localhost:27017'
        conn = pymongo.MongoClient(connection_string)
        db = conn[self.db]
        collection = db[self.collection]
        collection.insert_one(new_data)
        print("Data inserted successfully!")

    def update_database(self, profileUrl, new_data, area):
        connection_string = 'mongodb://localhost:27017'
        conn = pymongo.MongoClient(connection_string)
        db = conn[self.db]
        collection = db[self.collection]

        search_query = {"Field104": profileUrl}

        update_query = {
            "$set": {
                "Field3": new_data["Field3"],
                "Field102": new_data["Field102"],
                "Field104": new_data["Field104"]
            }
        }
        collection.update_one(search_query, update_query, upsert=True)
        print(f"Data updated successfully! + {area}")


    '''    HANDLING IMAGES - WASABI     '''
    def download_image(self, img_url, file_path, name):
        try:
            response = requests.request(method='GET', url=img_url)
            if response.status_code == 200:
                # sanitized_name = self.sanitize_filename(name)
                with open(f'{file_path}/{name}', 'wb') as file:
                    file.write(response.content)
                print(f"{name} Image downloaded successfully.")
            else:
                print("Failed to download the image. Status code:", response.status_code)
        except requests.exceptions.RequestException as e:
            print("An error occurred while downloading:", e)
        except Exception as e:
            print(e)

    def create_new_bucket(self, bucket_prefix, bucket_number, s3):
        new_bucket_name = f'{bucket_prefix}_{bucket_number}'
        s3.create_bucket(Bucket=new_bucket_name)
        return new_bucket_name

    def uploaded(self, list_of_img, names):
        list_images = [url.strip() for url in list_of_img.split(',')]
        wasabi_access_key = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
        wasabi_secret_key = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
        s3 = boto3.client(
            's3',
            aws_access_key_id=wasabi_access_key,
            aws_secret_access_key=wasabi_secret_key,
            endpoint_url="https://s3.ap-southeast-1.wasabisys.com",
        )
        # bucket_prefix = 'BLS'
        bucket_prefix = self.bucket_prefix
        bucket_number = 1
        current_bucket_name = f'{bucket_prefix}_{bucket_number}'

        # existing_buckets = [bucket['Name'] for bucket in s3.list_buckets()['Buckets']]
        existing_buckets = [bucket['Name'] for bucket in s3.list_buckets()['Buckets']]
        if current_bucket_name in existing_buckets:
            if current_bucket_name != self.create_new_bucket(bucket_prefix, bucket_number, s3):
                print("Bucket already exists. Create Buket and Run...")
                return

        current_bucket_name = self.create_new_bucket(bucket_prefix, bucket_number, s3)
        print(f"using bucket: {current_bucket_name}")
        try:
            object_count = len(s3.list_objects(Bucket=current_bucket_name).get('Contents', []))
        except s3.exceptions.NoSuchBucket:
            current_bucket_name = self.create_new_bucket(bucket_prefix, bucket_number, s3)
            object_count = 0
        wasabi_url = []

        # create image folder automacity
        current_directory = os.getcwd()
        image_folder = 'images'
        image_directory = os.path.join(current_directory, image_folder)
        os.makedirs(image_directory, exist_ok=True)

        for index, img in enumerate(list_images, start=1):
            try:
                image_url = img
                # local_file_path = 'C:/Users/Jahanzaib/Desktop/img/'
                local_file_path = os.path.join(image_directory).replace('\\', '/')

                title_name = f'{names}_{index}.jpg'

                img_url = f'https://s3.ap-southeast-1.wasabisys.com/{current_bucket_name}/{title_name}'
                if image_url:
                    self.download_image(image_url, local_file_path, title_name)
                    # download_image(image_url,a)

                if object_count >= 10000000:
                    bucket_number += 1
                    current_bucket_name = self.create_new_bucket(bucket_prefix, bucket_number, s3)
                    object_count = 0

                bucket_policy = {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": "*",
                            "Action": "s3:GetObject",
                            "Resource": f"arn:aws:s3:::{current_bucket_name}/*"
                        }
                    ]
                }

                s3.put_bucket_policy(Bucket=current_bucket_name, Policy=json.dumps(bucket_policy))
                # uploading
                file_path_on_disk = os.path.join(local_file_path, title_name)

                try:
                    content_type, _ = mimetypes.guess_type(file_path_on_disk)
                    s3.upload_file(
                        file_path_on_disk,
                        current_bucket_name,
                        title_name,
                        ExtraArgs={'ContentType': content_type} if content_type else None
                    )
                    self.delete_local_image(file_path_on_disk)
                except NoCredentialsError:
                    print("Credentials not available")
                except ClientError as e:
                    print(f"An error occurred: {e}")
                object_count += 1
                wasabi_url.append(img_url)
            except Exception as e:
                print(e)
        return wasabi_url

    def delete_local_image(self, file_path):
        try:
            os.remove(file_path)
        except OSError as e:
            print(f"Error deleting local image: {e}")


