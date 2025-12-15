################            ISale_Sale

import random
import mimetypes
import re
from botocore.exceptions import NoCredentialsError, ClientError
import requests, json, time, threading, queue, os
import boto3
import pymongo
from datetime import datetime
from botocore.exceptions import NoCredentialsError
import scrapy


class ISale_Sale(scrapy.Spider):
    name = 'ISale_Sale'
    prefix = 'https://www.isaleproperty.net.au'
    url = 'https://www.isaleproperty.net.au/propertiesJson?callback=angular.callbacks._0&currentPage={}&perPage=12&sort=d_listing%20desc&listing_cat=residential'
    headers = {
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.9,ur;q=0.8,nl;q=0.7',
        'sec-ch-ua': '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'script',
        'sec-fetch-mode': 'no-cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
    }

    count = 1
    db = 'ISale'
    collection = 'ISale_Sale'
    # bucket_prefix = f'D_{collection}'
    bucket_prefix = f'Images5'

    local_file_path = '/img'

    def __init__(self, *args, **kwargs):
        super(ISale_Sale, self).__init__(*args, **kwargs)
        self.links = []
        self.database_sale_matching_url = self.read_data_base()


    '''    SCRPAY SCRIPT START'S FROM HERE     '''
    def start_requests(self):
        page_no = 1
        yield scrapy.Request(url=self.url.format(page_no), callback=self.parse, headers=self.headers, meta={'page_no':page_no})

    def parse(self, response):
        json_strings = response.text
        json_string = json_strings.replace('None', 'null')
        start_index = json_string.find('{')
        end_index = json_string.rfind('}') + 1
        extracted_json = json_string[start_index:end_index]
        data = json.loads(extracted_json)

        '''    FARHAN'S LOGIC    '''
        all_links = response.css("a.propertyTile-address ::attr(href)").getall()
        links = []
        for each_url in data['rows']:
            each_url = self.prefix + each_url['fields']['link']
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


        for property in data['rows']:
            property_url = self.prefix + property['fields']['link']
            print(self.count, ' # Property URL is : ', property_url)
            self.count += 1

            mini_data = dict()
            mini_data['address'] = property.get('fields').get('address_compiled')
            mini_data['bed'] = property.get('fields').get('n_bedrooms')
            mini_data['bath'] = property.get('fields').get('n_bathrooms')
            mini_data['car'] = property.get('fields').get('n_car_spaces')
            mini_data['sold_price'] = property.get('fields').get('display_price').replace('SOLD','').replace('for','').replace(' ','')
            mini_data['property_type'] = ''
            if property.get('fields').get('categories'):
                mini_data['property_type'] = property.get('fields').get('categories')[0]
            self.Detail_parse(mini_data,property)

        '''       Pagination     '''
        page_no = data['currentPage']
        total_results = data['paginationParams']['totalPages']
        if int(page_no) < int(total_results):
            page_no = int(data['currentPage']) + 1
            yield scrapy.Request(url=self.url.format(page_no), callback=self.parse, headers=self.headers,
                                 meta={'page_no': page_no})


    def Detail_parse(self, mini_data,full_data):
        detail_page_url = self.prefix + full_data['fields']['link']
        database_sale_matching_url = self.read_data_base()
        if detail_page_url not in database_sale_matching_url:
            mini_data = mini_data
            full_data = full_data
            item = dict()
            item['Field1'] = ''
            item['Field2'] = int(3315)
            item['Field3'] = datetime.now().date().strftime('%Y-%m-%d')  # date_data_scanned
            item['Field4'] = 'I-Sale Property'

            item['Field5'] = mini_data.get('address')
            item['Field6'] = mini_data.get('bed')
            item['Field7'] = mini_data.get('bath')
            item['Field8'] = mini_data.get('car')
            item['Field9'] = mini_data.get('sold_price').replace('$','').replace('-','').replace(' ','').replace('SOLD','')

            html_string = full_data.get('fields').get('desc_full_html').strip()
            item['Field12'] = re.sub('<.*?>', '', html_string)

            '''         Uploading Images on Wasabi S3 Bucket            '''
            images = []
            for img in full_data.get('fields').get('images'):
                img = img.get('url').replace('background-image:url(','').replace(");","")
                images.append(f'{img}' if img else "")
            images_string = ', '.join(images)
            # item['Field13'] = images_string
            # print('Property images are :', images_string)
            random_number = random.randint(1, 10000000000)
            item['Field13'] = ", ".join(self.uploaded(images_string, random_number))

            item['Field14'] = self.prefix + full_data['fields']['link']  # external_link
            '''          AGENTS          '''
            if full_data.get('fields').get('consultants'):
                agent1 = full_data.get('fields').get('consultants')[0]
                agent1_name = agent1.get('name')
                if agent1:
                    first_name1, last_name1 = agent1_name.split(maxsplit=1)
                    item['Field15'] = first_name1.strip()  # agent_first_name_1
                    item['Field16'] = last_name1.strip()  # agent_surname_name_1
                    item['Field17'] = agent1_name.strip()  # agent_full_name_1
                    item['Field18'] = agent1.get('position')
                    item['Field19'] =  agent1.get("primary_email")
                    agent1_phone = agent1.get("primary_phone")
                    item['Field20'] = agent1_phone.strip()
                    '''         Uploading Images on Wasabi S3 Bucket            '''
                    agent_image = agent1.get("imgSrc")
                    images_string = f'{agent_image}' if agent_image else ""
                    # item['Field22'] = images_string
                    # print('Agent1 image is :',images_string)
                    random_number = random.randint(1, 10000000000)
                    item['Field22'] = ",".join(self.uploaded(images_string, random_number))

            if len(full_data.get('fields').get('consultants')) > 1:
                agent2 = full_data.get('fields').get('consultants')[1]
                agent2_name = agent2.get('name')
                if agent2:
                    item['Field23'] = agent2_name.strip()
                    item['Field24'] = agent2.get('position')
                    item['Field25'] = agent2.get("primary_email")
                    agent2_phone = agent2.get("primary_phone")
                    item['Field26'] = agent2_phone
                    '''         Uploading Images on Wasabi S3 Bucket            '''
                    agent_image = agent2.get("imgSrc")
                    images_string = f'{agent_image}' if agent_image else ""
                    # item['Field27'] = images_string
                    # print('Agent2 image is :',images_string)
                    random_number = random.randint(1, 10000000000)
                    item['Field27'] = ",".join(self.uploaded(images_string, random_number))

            if len(full_data.get('fields').get('consultants')) == 3:
                agent3 = full_data.get('fields').get('consultants')[2]
                agent3_name = agent3.get('name')
                if agent3:
                    item['Field28'] = agent3_name.strip()
                    item['Field29'] = agent3.get('position')
                    item['Field30'] = agent3.get("primary_email")
                    agent3_phone = agent3.get("primary_phone")
                    item['Field31'] = agent3_phone.strip()
                    '''         Uploading Images on Wasabi S3 Bucket            '''
                    agent_image = agent3.get("imgSrc")
                    images_string = f'{agent_image}' if agent_image else ""
                    # item['Field32'] = images_string
                    # print('Agent3 image is :',images_string)
                    random_number = random.randint(1, 10000000000)
                    item['Field32'] = ", ".join(self.uploaded(images_string, random_number))


            item['Field33'] = full_data.get('fields').get('id')
            item['Field35'] = mini_data.get('property_type')
            item['Field36'] = full_data.get('fields',{}).get('land_area_m2')
            item['Field37'] = full_data.get('fields',{}).get('floor_area_m2')

            # feature_count = 58
            # for feature in response.css(".epl-property-features li"):   ##  first 4 features are ID, bed, bath etc.
            #     if 'condition' in feature.css('::text').get('').strip().lower():
            #         item['Field52'] = feature.css('::text').get('').strip()
            #     else:
            #         item[f'Field{feature_count}'] = feature.css('::text').get('').strip()
            #         feature_count += 1

            ####        ACTIVE / REMOVED Logic implemented in "Field102"
            item['Field102'] = 'ACTIVE'
            item['Field104'] = item['Field14']
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
        print(f"Data updated successfully! + {area} : ",search_query)


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


