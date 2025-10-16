##########      BigginScott_Sale

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


class BigginScott_Sale(scrapy.Spider):
    name = "BigginScott_Sale"

    url = 'https://bigginscott.com.au/propertiesJson?callback=angular.callbacks._2&currentPage={}&perPage=96&sort=d_listing%20desc&listing_cat=residential'
    prefix = "https://bigginscott.com.au"
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
    data = {
            'atts[list]': 'sold',
            'atts[multilist]': '',
            'atts[layout]': '',
            'atts[template]': 'Shortcode.SearchResults.SearchResults',
            'atts[selector_listings]': 'ap-listing-search-results',
            'atts[ajax_template]': 'Ajax.SearchResults',
            'atts[load_more]': 'true',
            'atts[hide_search_form]': '0',
            'atts[row_col_class]': 'row row-cols-xl-3 row-cols-lg-2 row-cols-1',
            'atts[per_page]': '18',
            'atts[sur_suburbs]': '0',
            'atts[center_latlng]': '',
            'atts[sur_suburbs_radius]': '10',
            'atts[max_page]': 'false',
            'atts[map_zoom]': '11',
            'atts[map]': 'map_canvas',
            'atts[map_load_all_marker]': '1',
            'atts[map_callback]': 'ap_realty.searchResultsMapCallback',
            'atts[map_attribute_cluster]': '0',
            'atts[map_is_visible]': '1',
            'atts[property_type_column]': '1,2,3',
            'atts[content_first]': '0',
            'atts[auth]': '0',
            'atts[auth_type]': 'content',
            'atts[auth_message]': 'Sorry, You Are Not Allowed to Access This Page',
            'atts[include_private_listing]': '0',
            'atts[map_attribute_loadCallback]': 'ap_realty.searchResultsMapCallback',
            'atts[load_more_automatic]': 'true',
            'atts[sort]': 'sold_date_desc',
            'per_page': '18',
            'load_more': 'true',
            'selectorMap': '#map_canvas',
            'selector': '#ap-listing-search-results',
            'current_page': '2',
            'sort': 'sold_date_desc',
            'action': 'property_search_results',
        }

    count = 1
    db = 'BigginScott'
    collection = 'BigginScott_Sale'
    # bucket_prefix = f'LRE_{collection}'
    bucket_prefix = f'Images3'

    local_file_path = '/img'

    def __init__(self, *args, **kwargs):
        super(BigginScott_Sale, self).__init__(*args, **kwargs)
        self.links = []
        self.database_sale_matching_url = self.read_data_base()


    '''    SCRPAY SCRIPT START'S FROM HERE     '''
    def start_requests(self):
        page_no = 1
        yield scrapy.Request(self.url.format(page_no), headers=self.headers, callback=self.parse, meta={'page_no':page_no})


    def parse(self, response):
        '''    FARHAN'S LOGIC    '''
        data = json.loads(response.text.replace('/**/','').replace('angular.callbacks._2(','').replace(');',''))
        links = []
        for each_url in data.get('rows',[]):
            each_url = f'https://bigginscott.com.au{each_url.get("fields",{}).get("link","")}'
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


        for property in data.get('rows',[]):
            property_url = f'https://bigginscott.com.au{property.get("fields",{}).get("link","")}' if property.get("fields",{}).get("link","") else ''
            print(self.count,' # Property URL :', property_url)
            self.count += 1
            # yield response.follow(property_url, headers=self.headers, callback=self.detail_parse, meta={'property':property})
            self.detail_parse(property.get('fields'))

        page_no = response.meta['page_no']
        total_pages = data.get('paginationParams', {}).get('totalPages')
        print(f"Current-Page = {page_no} || Total-Pages = {total_pages}")
        if page_no < total_pages:
            page_no += 1
            yield scrapy.Request(self.url.format(page_no), headers=self.headers, callback=self.parse, meta={'page_no': page_no})


    # def detail_parse(self, response):
    def detail_parse(self, property):
        detail_page_url = f'https://bigginscott.com.au{property.get("link", "")}'
        property_url = f'https://bigginscott.com.au{property.get("link", "")}'
        database_sale_matching_url = self.read_data_base()
        if detail_page_url not in database_sale_matching_url:
            item = dict()
            item['Field1'] = ''
            item['Field2'] = int(2364)
            item['Field3'] = datetime.now().date().strftime('%Y-%m-%d')  # date_data_scanned
            item['Field4'] = 'Biggin Scott'

            address = property.get('address_compiled','')   # full_adddress
            postcode = property.get('xxxxxxxxx','')   # full_adddress
            item['Field5'] = f"{address} {postcode}"   # full_adddress

            item['Field6'] = property.get('n_bedrooms','')  # bedrooms
            item['Field7'] = property.get('n_bathrooms','')  # bathrooms
            item['Field8'] = property.get('n_car_spaces','')    # car_spaces

            item['Field9'] = property.get('display_price','').replace('SOLD','').replace('for','').replace(',','').replace('$','').replace(' ','')  # price
            item['Field12'] = property.get('desc_preview','').replace('\n','').replace('\t','').replace('\r','')  # price

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
            if property.get("consultants"):
                agent1 = property.get("consultants")[0]
                agent1_name = agent1.get('name','')
                if agent1_name:
                    first_name1, last_name1 = agent1_name.split(maxsplit=1)
                    item['Field15'] = first_name1.strip()  # agent_first_name_1
                    item['Field16'] = last_name1.strip()  # agent_surname_name_1
                    item['Field17'] = agent1_name.strip()  # agent_full_name_1
                    item['Field18'] = agent1.get('position')
                    item['Field19'] = agent1.get('primary_email')
                    item['Field20'] = agent1.get('primary_phone')
                    item['Field21'] = ' '.join(agent1.get('work_phone')) if agent1.get('work_phone') else ''
                    '''         Uploading Images on Wasabi S3 Bucket            '''
                    agent_image = agent1.get('imgSrc')
                    images_string = f'{agent_image}' if agent_image else ""
                    # item['Field22'] = images_string
                    # print('Agent1 image is :',images_string)
                    random_number = random.randint(1, 10000000000)
                    item['Field22'] = ",".join(self.uploaded(images_string, random_number))

            if property.get("consultants"):
                if len(property.get("consultants")) > 1:
                    agent2 = property.get("consultants")[1]
                    agent2_name = agent2.get('name','')
                    if agent2:
                        item['Field23'] = agent2_name.strip()
                        item['Field24'] = agent2.get('position')
                        item['Field25'] = agent2.get('primary_email')
                        item['Field26'] = agent2.get('primary_phone')
                        item['Field26A'] = ' '.join(agent2.get('work_phone')) if agent2.get('work_phone') else ''
                        '''         Uploading Images on Wasabi S3 Bucket            '''
                        agent_image = agent2.get('imgSrc')
                        images_string = f'{agent_image}' if agent_image else ""
                        # item['Field27'] = images_string
                        # print('Agent2 image is :',images_string)
                        random_number = random.randint(1, 10000000000)
                        item['Field27'] = ",".join(self.uploaded(images_string, random_number))

                if len(property.get("consultants")) > 2:
                    agent3 = property.get("consultants")[2]
                    agent3_name = agent3.get('name','')
                    if agent3:
                        item['Field28'] = agent3_name.strip()
                        item['Field29'] = agent3.get('position')
                        item['Field30'] = agent3.get('primary_email')
                        item['Field31'] = agent3.get('primary_phone')
                        item['Field31A'] = ' '.join(agent3.get('work_phone')) if agent3.get('work_phone') else ''
                        '''         Uploading Images on Wasabi S3 Bucket            '''
                        agent_image = agent3.get('imgSrc')
                        images_string = f'{agent_image}' if agent_image else ""
                        # item['Field32'] = images_string
                        # print('Agent3 image is :',images_string)
                        random_number = random.randint(1, 10000000000)
                        item['Field32'] = ", ".join(self.uploaded(images_string, random_number))


            item['Field33'] = property.get('id','')
            item['Field35'] = ''.join(property.get('categories')) if property.get('categories') else ''
            item['Field36'] = property.get('land_area_m2','')
            # item['Field37'] = property.get('floor_area','')

            # feature_count = 58
            # for feature in response.css(".epl-property-features li"):   ##  first 4 features are ID, bed, bath etc.
            #     if 'condition' in feature.css('::text').get('').strip().lower():
            #         item['Field52'] = feature.css('::text').get('').strip()
            #     else:
            #         item[f'Field{feature_count}'] = feature.css('::text').get('').strip()
            #         feature_count += 1

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


