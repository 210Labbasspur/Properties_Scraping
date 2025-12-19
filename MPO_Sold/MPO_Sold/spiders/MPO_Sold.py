#####################333            MPO_Sold

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


class MPO_Sold(scrapy.Spider):
    name = "MPO_Sold"

    url = 'https://www.mpo.net.au/sold'
    prefix = "https://www.mpo.net.au"
    headers = {
        'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8,ru;q=0.7',
        'priority': 'u=0, i',
        'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    }

    count = 1
    db = 'MPO'
    collection = 'MPO_Sold'
    # bucket_prefix = f'LRE_{collection}'
    bucket_prefix = f'Images4'

    local_file_path = '/img'

    def __init__(self, *args, **kwargs):
        super(MPO_Sold, self).__init__(*args, **kwargs)
        self.links = []
        self.database_sale_matching_url = self.read_data_base()


    '''    SCRPAY SCRIPT START'S FROM HERE     '''
    def start_requests(self):
        yield scrapy.Request(self.url, headers=self.headers, callback=self.parse)


    def parse(self, response):
        '''    FARHAN'S LOGIC    '''
        json_text = response.xpath("//*[contains(text(),'commitToSoldListingsStore')]/text()").get('').strip()
        # Regex pattern to extract the content between "/**/commitToSoldListingsStore({" and "});"
        pattern = r'\/\*\*\/commitToSoldListingsStore\(\{(.*?)\}\);'
        match = re.search(pattern, json_text, re.DOTALL)
        if match:
            extracted_string = match.group(1)
            data = json.loads('{' + extracted_string + '}')
            links = []
            for each_url in data.get('data',[]):
                url_slug = each_url.get("slug","")
                each_url = f'https://www.mpo.net.au/sold/{url_slug}'
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


            for property in data.get('data',[]):
                url_slug = property.get("slug","")
                property_url = f'https://www.mpo.net.au/sold/{url_slug}'
                print(self.count,' # Property URL :', property_url)
                self.count += 1
                self.detail_parse(property)

        else:
            print("No match found || Couldnt read and convert the json.")


    # def detail_parse(self, response):
    def detail_parse(self, property):
        url_slug = property.get("slug", "")
        property_url = f'https://www.mpo.net.au/sold/{url_slug}'
        detail_page_url = property_url
        database_sale_matching_url = self.read_data_base()
        if detail_page_url not in database_sale_matching_url:
            item = dict()
            item['Field1'] = ''
            item['Field2'] = int(2164)
            item['Field3'] = datetime.now().date().strftime('%Y-%m-%d')  # date_data_scanned
            item['Field4'] = 'Mareeba Property Office'

            street_number = property.get('street_number','')   # full_adddress
            street = property.get('street','')   # full_adddress
            suburb = property.get('suburb','')   # full_adddress
            item['Field5'] = f"{street_number} {street} {suburb}"   # full_adddress

            item['Field6'] = property.get('bedrooms','')  # bedrooms
            item['Field7'] = property.get('bathrooms','')  # bathrooms
            item['Field8'] = property.get('car_spaces','')    # car_spaces

            item['Field9'] = property.get('price_sold','')
            item['Field12'] = property.get('description','').replace('\n','').replace('\t','').replace('\r','')  # price

            '''         Uploading Images on Wasabi S3 Bucket            '''
            prop_images = property.get('images',[])
            images = []
            for img in prop_images:
                img = img.get('url','').replace('background-image:url(','').replace(");","")
                images.append(f'{img}' if img else "")
            images_string = ', '.join(images)
            # item['Field13'] = images_string
            # print('Property images are :', images_string)
            random_number = random.randint(1, 10000000000)
            item['Field13'] = ", ".join(self.uploaded(images_string, random_number))


            item['Field14'] = property_url  # external_link
            '''          AGENTS          '''
            if property.get("listing_agents"):
                agent1 = property.get("listing_agents")[0]
                agent1_name = agent1.get('name','')
                if agent1_name:
                    first_name1, last_name1 = agent1_name.split(maxsplit=1)
                    item['Field15'] = first_name1.strip()  # agent_first_name_1
                    item['Field16'] = last_name1.strip()  # agent_surname_name_1
                    item['Field17'] = agent1_name.strip()  # agent_full_name_1
                    item['Field19'] = agent1.get('email','')
                    item['Field20'] = agent1.get('telephone','')
                    '''         Uploading Images on Wasabi S3 Bucket            -   No Image on website JSON '''

            if property.get("listing_agents"):
                if len(property.get("listing_agents")) > 1:
                    agent2 = property.get("listing_agents")[1]
                    agent2_name = agent2.get('name','')
                    if agent2:
                        item['Field23'] = agent2_name.strip()
                        item['Field25'] = agent2.get('email')
                        item['Field26'] = agent2.get('telephone')
                        '''         Uploading Images on Wasabi S3 Bucket            -   No Image on website JSON '''

                if len(property.get("listing_agents")) > 2:
                    agent3 = property.get("listing_agents")[2]
                    agent3_name = agent3.get('name','')
                    if agent3:
                        item['Field28'] = agent3_name.strip()
                        item['Field30'] = agent3.get('email')
                        item['Field31'] = agent3.get('telephone')
                        '''         Uploading Images on Wasabi S3 Bucket            -   No Image on website JSON '''


            item['Field33'] = property.get('id','')
            # item['Field35'] = ''.join(property.get('categories')) if property.get('categories') else ''
            item['Field36'] = property.get('land_area','')
            # item['Field37'] = property.get('floor_area','')

            if property.get('features',[]):
                print('Features are : ',property.get('features',[]))
                feature_count = 58
                for feature in property.get('features',[]):   ##  first 4 features are ID, bed, bath etc.
                    item[f'Field{feature_count}'] = feature
                    feature_count += 1

            ####        ACTIVE / REMOVED Logic implemented in "Field102"
            item['Field102'] = 'ACTIVE'
            item['Field104'] = property_url
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
            # sale = each_row.get('Field104')
            sale = each_row.get('Field14')
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

        # search_query = {"Field104": profileUrl}
        search_query = {"Field14": profileUrl}

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



