#################       Mclaren_Rent

import re
import scrapy
import pymongo
import mimetypes
from copy import deepcopy
from datetime import datetime
import requests, json, time, threading, queue, os
import boto3
from botocore.exceptions import NoCredentialsError, ClientError

class Mclaren_Rent(scrapy.Spider):
    name = 'Mclaren_Rent'
    prefix = 'https://www.mclarenrealestate.net.au'
    prop_prefix = 'https://www.mclarenrealestate.com.au/property/'
    url = 'https://www.mclarenrealestate.com.au/api/get-properties'
    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'en-US,en;q=0.9,ur;q=0.8,nl;q=0.7',
        'content-type': 'application/json',
        # 'cookie': '_gcl_au=1.1.650692317.1713705258; _gid=GA1.3.2045880784.1714230545; _ce.clock_event=1; _ce.clock_data=-785%2C182.186.65.47%2C1%2Ca16ddaab909d2cf27fce353f26dd2ff2; _ce.irv=returning; cebs=1; _gat_gtag_UA_83617516_1=1; _gat_UA-83617516-1=1; arp_scroll_position=2057; _ga_RWJKS636VT=GS1.1.1714262399.10.1.1714264915.7.0.0; _uetsid=1823074004a811ef97dc812f20992755; _uetvid=0f2f3d70ffe111ee9b701b848c74c1e5; _ga=GA1.3.1643173684.1713705258; cebsp_=9; _ce.s=v~d51db85f2f7ea75615e0e3ff2774da9c5d5ac21d~lcw~1714264917886~lva~1714262404258~vpv~4~v11.fhb~1714262404285~v11.lhb~1714264917878~v11.cs~367534~v11.s~441c0b20-04f2-11ef-81f2-75ef812b276c~v11.sla~1714264912598~lcw~1714264917889',
        'origin': 'https://www.mclarenrealestate.com.au',
        'priority': 'u=1, i',
        'referer': 'https://www.mclarenrealestate.com.au/for-rent',
        'sec-ch-ua': '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    }

    json_data = {
        'limit': 8,
        'adaAgent': True,
        'page': 1,
        'status': 'ACTIVE',
        'listingType': 'RESIDENTIAL_RENTAL',
    }

    count = 1
    db = 'Mclaren'
    collection = 'Mclaren_Rent'
    bucket_prefix = f'D_{collection}'

    def start_requests(self):
        count_url = 'https://www.mclarenrealestate.com.au/api/get-count-property'
        yield scrapy.Request(url=count_url, method='POST', body=json.dumps(self.json_data), callback=self.parse, headers=self.headers)

    def parse(self, response):
        data = json.loads(response.text)
        Total_P = data['total']
        print('Total Properties are : ', Total_P)
        page_no = 1
        Property_No = 0
        payload = deepcopy(self.json_data)
        payload['page'] = page_no
        yield scrapy.Request(url=self.url, method='POST', body=json.dumps(payload), callback=self.parse_listing, headers=self.headers,
                             meta={'page_no':page_no,'Property_No':Property_No,'Total_P':Total_P})

    def parse_listing(self, response):
        Property_No = response.meta['Property_No']
        data = json.loads(response.text)
        for property in data['dataProperies']:
            Property_No += 1
            property_url = self.prop_prefix + property.get('id')
            print(self.count, property_url)
            self.count += 1
            mini_data = dict()
            mini_data['address'] = property.get('address').get('formattedAddress')
            mini_data['bed'] = property.get('listingDetails').get('bedrooms')
            mini_data['bath'] = property.get('listingDetails').get('bathrooms')
            carport_spaces = property.get('listingDetails').get('carportSpaces')
            garage_spaces = property.get('listingDetails').get('garageSpaces')
            mini_data['car'] = (carport_spaces or 0) + (garage_spaces or 0)
            mini_data['sold_price'] = property.get('soldPrice')
            mini_data['land_area'] = property.get('landSize')

            self.Detail_parse(mini_data, property)

        '''     Pagination     '''
        Total_P = response.meta['Total_P']
        if Property_No < Total_P:
            page_no = response.meta['page_no'] + 1
            payload = deepcopy(self.json_data)
            payload['page'] = page_no
            print('New Page No is : ', page_no)
            yield scrapy.Request(url=self.url, method='POST', body=json.dumps(payload), callback=self.parse_listing, headers=self.headers,
                                 meta={'page_no': page_no, 'Property_No': Property_No, 'Total_P': Total_P})

    def Detail_parse(self, mini_data, property):
        url = self.prop_prefix + property.get('id')
        DB_already_exists = self.read_data_base(url)
        if not DB_already_exists:
            item = dict()
            full_data = property
            item['Field2'] = '1324'
            item['Field3'] = datetime.now().date().strftime('%Y-%m-%d')  # date_data_scanned
            item['Field4'] = 'Mclaren Real Estate'

            item['Field5'] = mini_data['address']  # full_adddress
            item['Field6'] = mini_data['bed']
            item['Field7'] = mini_data['bath']
            item['Field8'] = mini_data['car']
            # item['Field9'] = mini_data['sold_price']

            '''         Uploading Images on Wasabi S3 Bucket            '''
            new_img_urls = []
            for url in full_data.get('images'):
                new_img_urls.append(url.get('url'))
            Images = ', '.join(new_img_urls)
            new_name = self.db.replace(' ', '').replace('/', '_')
            id = property.get('id')
            # item['Field13'] = new_img_urls   # listing_images
            # print(Images,new_name,id)
            images = self.uploaded(Images, new_name, id)  ## ','saperated Images(string), Property_address, URL_id
            item['Field13'] = ", ".join(images)

            item['Field14'] = self.prop_prefix + property.get('id')  # external_link

            '''          AGENTS          '''
            if full_data['agents']:
                agent1 = full_data['agents'][0]
                agent1_name = agent1.get('name')
                if agent1:
                    first_name1, last_name1 = agent1_name.split(maxsplit=1)
                    item['Field15'] = first_name1.strip()  # agent_first_name_1
                    item['Field16'] = last_name1.strip()  # agent_surname_name_1
                    item['Field17'] = agent1_name.strip()  # agent_full_name_1
                    '''         Uploading Images on Wasabi S3 Bucket            '''
                    Agent_Images = agent1.get('avatarUrl')
                    new_name = agent1_name.replace(' ', '')
                    id = agent1.get('id')
                    # item['Field22'] = Agent_Images   # listing_images
                    # print(f'Agent1 : {Agent_Images}, {new_name}, {id}')
                    images = self.uploaded(Agent_Images, new_name, id)  ## ','saperated Images(string), agent_name, agent_id
                    item['Field22'] = ", ".join(images)

                if len(full_data) > 1:
                    agent2 = full_data['agents'][1]
                    agent2_name = agent2.get('name')
                    if agent2:
                        item['Field23'] = agent2_name.strip()
                        '''         Uploading Images on Wasabi S3 Bucket            '''
                        Agent_Images = agent2.get('avatarUrl')
                        new_name = agent2_name.replace(' ', '')
                        id = agent2.get('id')
                        # item['Field27'] = Agent_Images
                        # print(f'Agent2 : {Agent_Images}, {new_name}, {id}')
                        images = self.uploaded(Agent_Images, new_name, id)  ## ','saperated Images(string), agent_name, agent_id
                        item['Field27'] = ", ".join(images)

                if len(full_data) > 2:
                    agent3 = full_data['agents'][2]
                    agent3_name = agent3.get('name')
                    if agent3:
                        item['Field28'] = agent3_name.strip()
                        '''         Uploading Images on Wasabi S3 Bucket            '''
                        Agent_Images = agent3.get('avatarUrl')
                        new_name = agent3_name.replace(' ', '')
                        id = agent3.get('id')
                        # item['Field32'] = Agent_Images
                        # print('Agent3 :',Agent_Images,',', new_name,',', id)
                        images = self.uploaded(Agent_Images, new_name, id)  ## ','saperated Images(string), agent_name, agent_id
                        item['Field32'] = ", ".join(images)

            # prop_id = response.xpath("//*[contains(text(),'property ID')]/parent::div[1]/text()").getall()
            # item['Field33'] = prop_id[-1].strip()  # description
            item['Field35'] = full_data.get('propertyType')
            item['Field36'] = mini_data['land_area']

            feature_count = 58
            for feature in full_data.get("listingDetails").get('indoorFeatures'):
                if feature == 'Air Condition':
                    item['Field52'] = feature
                else:
                    item[f'Field{feature_count}'] = feature
                    feature_count += 1
            feature_count = 65
            for feature in full_data.get("listingDetails").get('outdoorFeatures'):
                item[f'Field{feature_count}'] = feature
                feature_count += 1

            print(item)
            self.insert_database(item)
        else:
            print('Data already exists')

    def download_image(self, img_url, file_dir, name):
        print('Download image details are : ',img_url, file_dir, name)
        try:
            response = requests.request(method='GET', url=img_url)
            if response.status_code == 200:
                if not os.path.exists(file_dir):
                    os.makedirs(file_dir)
                with open(os.path.join(file_dir, name), 'wb') as file:
                    file.write(response.content)
                print(f"{name} Image downloaded successfully.")
                return True
            else:
                print(f"Failed to download the image {img_url}. Status code:", response.status_code)
                return None
        except requests.exceptions.RequestException as e:
            print("An error occurred while downloading:", e)
        except Exception as e:
            print(e)

    def create_new_bucket(self, bucket_prefix, bucket_number, s3):
        new_bucket_name = f'{bucket_prefix}_{bucket_number}'
        s3.create_bucket(Bucket=new_bucket_name)
        return new_bucket_name

    def uploaded(self, list_of_img, names, id):
        list_images = [url.strip() for url in list_of_img.split(',')]

        wasabi_access_key = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
        wasabi_secret_key = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
        s3 = boto3.client(
            's3',
            aws_access_key_id=wasabi_access_key,
            aws_secret_access_key=wasabi_secret_key,
            endpoint_url="https://s3.ap-southeast-1.wasabisys.com",
        )
        # bucket_prefix = 'D_sales_properties'
        bucket_prefix = self.bucket_prefix
        bucket_number = 1
        current_bucket_name = f'{bucket_prefix}_{bucket_number}'

        existing_buckets = [bucket['Name'] for bucket in s3.list_buckets()['Buckets']]
        if current_bucket_name in existing_buckets:
            if current_bucket_name != self.create_new_bucket(bucket_prefix, bucket_number, s3):
                print("Bucket already exists. Create Buket and Run...")
                return

        current_bucket_name = self.create_new_bucket(bucket_prefix, bucket_number, s3)
        print(f"Created and using bucket: {current_bucket_name}")
        try:
            object_count = len(s3.list_objects(Bucket=current_bucket_name).get('Contents', []))
        except s3.exceptions.NoSuchBucket:
            current_bucket_name = self.create_new_bucket(bucket_prefix, bucket_number, s3)
            object_count = 0
        wasabi_url = []

        for index, img in enumerate(list_images, start=1):
            image_url = img

            local_file_path = '/img'

            title_name = f'{names}_{id}_{index}.jpg'

            img_url = f'https://s3.ap-southeast-1.wasabisys.com/{current_bucket_name}/{title_name}'
            check_img = self.download_image(image_url, local_file_path, title_name)
            if check_img:
                if object_count >= 100000000:
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
        return wasabi_url

    def delete_local_image(self, file_path):
        try:
            os.remove(file_path)
        except OSError as e:
            print(f"Error deleting local image: {e}")

    def read_data_base(self, profileUrl):
        url = profileUrl
        connection_string = 'mongodb://localhost:27017'
        conn = pymongo.MongoClient(connection_string)
        db = conn[self.db]
        collection = db[self.collection]
        search_query = {"Field14": url}

        sale_urls_list_of_DB = []
        all_matching_data = collection.find_one(search_query)

        if all_matching_data:
            print(all_matching_data.get('Field5'))
            return True
        else:
            return False

    def insert_database(self, new_data):
        connection_string = 'mongodb://localhost:27017'
        conn = pymongo.MongoClient(connection_string)
        db = conn[self.db]
        collection = db[self.collection]

        collection.insert_one(new_data)
        print("Data inserted successfully!")
