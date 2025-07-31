#######     SP_Rent

import re
import csv
import copy
import scrapy
import pymongo
import mimetypes
from copy import deepcopy
from parsel import Selector
from datetime import datetime
import requests, json, time, threading, queue, os
import boto3
from botocore.exceptions import NoCredentialsError
from botocore.exceptions import NoCredentialsError, ClientError
class SP_Rent(scrapy.Spider):
    name = 'SP_Rent'
    prefix = 'https://www.strathfieldpartners.com.au'
    url = "https://www.strathfieldpartners.com.au/wp-admin/admin-ajax.php"
    cookies = {
        '_ga': 'GA1.1.1918580732.1712492073',
        '_ga_B58XQFHS87': 'GS1.1.1712492073.1.1.1712492439.0.0.0',
        'arp_scroll_position': '1900',
    }
    headers = {
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.9,ur;q=0.8,nl;q=0.7',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        # 'cookie': '_ga=GA1.1.1918580732.1712492073; _ga_B58XQFHS87=GS1.1.1712492073.1.1.1712492439.0.0.0; arp_scroll_position=1900',
        'origin': 'https://www.strathfieldpartners.com.au',
        'referer': 'https://www.strathfieldpartners.com.au/selling/recent-sales/',
        'sec-ch-ua': '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
        'x-requested-with': 'XMLHttpRequest',
    }
    data = {
        'atts[list]': 'lease',
        'per_page': '',
        'current_page': '',

        'atts[multilist]': '',        'atts[layout]': '',        'atts[per_page]': '6',
        'atts[template]': 'Shortcode.SearchResults.SearchResults',        'atts[selector_listings]': 'ap-listing-search-results',
        'atts[ajax_template]': 'Ajax.SearchResults',        'atts[load_more]': 'true',        'atts[hide_search_form]': '0',
        'atts[row_col_class]': 'row row-cols-xl-3 row-cols-lg-2 row-cols-1',        'atts[max_page]': 'false',        'atts[map_zoom]': '11',
        'atts[sur_suburbs]': '0',        'atts[center_latlng]': '',        'atts[sur_suburbs_radius]': '10',
        'atts[map]': 'map_canvas',        'atts[map_load_all_marker]': '0',        'atts[map_callback]': 'ap_realty.searchResultsMapCallback',
        'atts[map_attribute_cluster]': '0',        'atts[map_is_visible]': '1',        'atts[property_type_column]': '1,2,3',
        'atts[content_first]': '0',        'atts[auth]': '0',        'atts[auth_type]': 'content',
        'atts[auth_message]': 'Sorry, You Are Not Allowed to Access This Page',        'atts[include_private_listing]': '0',
        'atts[map_attribute_loadCallback]': 'ap_realty.searchResultsMapCallback',        'atts[sort]': 'sold_date_desc',
        'load_more': 'true',        'selectorMap': '#map_canvas',        'selector': '#ap-listing-search-results',
        'sort': 'sold_date_desc',        'action': 'property_search_results',
    }

    count = 1
    db = 'SP'
    collection = 'SP_Rent'
    bucket_prefix = f'D_{collection}'

    def start_requests(self):
        payload = deepcopy(self.data)
        payload['per_page'] = '100'
        page_no = 1
        payload['current_page'] = str(page_no)
        yield scrapy.FormRequest(url=self.url, formdata=payload, method='POST', callback=self.parse, headers=self.headers,
                                 meta={'page_no':page_no, 'payload':payload})

    def parse(self, response):
        data = json.loads(response.text)
        for property in data['data']['props']:
            property_url = property.get('url')
            print(self.count, property_url)
            self.count += 1

            item = dict()
            item['Field2'] = '1056'
            item['Field3'] = datetime.now().date().strftime('%Y-%m-%d')  # date_data_scanned
            item['Field4'] = self.db
            item['Field5'] =  property.get('address')
            item['Field6'] =  property.get('bedrooms')
            item['Field7'] =  property.get('bathrooms')
            item['Field8'] =  property.get('garage')
            item['Field9'] =  property.get('sold_price')
            item['Field12'] =  property.get('description').replace('\n',' ').replace('\t',' ')
            yield scrapy.Request(url=property_url, headers=self.headers, callback=self.Detail_parse, meta={'item':item})

        total = data['data']['total_pages']
        page_no = response.meta['page_no']
        if page_no < total:
            payload = response.meta['payload']
            page_no = page_no + 1
            payload['current_page'] = str(page_no)
            yield scrapy.FormRequest(url=self.url, formdata=payload, method='POST', callback=self.parse, headers=self.headers,
                                     meta={'page_no': page_no, 'payload': payload})

    def Detail_parse(self, response):
        DB_already_exists = self.read_data_base(response.url)
        if not DB_already_exists:
            item = response.meta['item']
            property_add = response.xpath("//*[contains(@class,'property-address text-center color-primary')]/text()").getall()  # full_adddress
            property_address = ' '.join(element.strip().replace('\t', ' ') for element in property_add)  # description

            item['Field5'] = property_address.strip()  # full_adddress

            # item['Field6'] = response.xpath("//span[contains(text(),'Bedrooms')]/parent::li[1]/text()").get('').strip()
            # item['Field7'] = response.xpath("//span[contains(text(),'Bathrooms')]/parent::li[1]/text()").get('').strip()
            # item['Field8'] = response.xpath("//span[contains(text(),'Garages')]/parent::li[1]/text()").get('').strip()
            #
            # sold_price = response.xpath("//*[contains(@data-testid,'listing-price')]/text()").get('').strip()
            # if sold_price:
            #     item['Field9'] = (sold_price.strip()).replace('$','')   # sold_price
            #
            # description = response.css('.section p::text , strong ::text').getall()  # description
            # item['Field12'] = ' '.join(element.strip().replace('\t', ' ') for element in description)  # description

            '''         Uploading Images on Wasabi S3 Bucket            '''
            Agent_Imagess = response.css('.img-default-used-size-medium ::attr(src)').getall()
            new_img_urls = []
            for url in Agent_Imagess:
                new_img_urls.append(url)
            Images = ', '.join(new_img_urls)
            new_name = property_address.replace(' ', '').replace('/', '_')
            id = (response.url).split('/')[-1]
            # item['Field13'] = new_img_urls   # listing_images
            # print(Images,new_name,id)
            images = self.uploaded(Images, new_name, id)  ## ','saperated Images(string), Property_address, URL_id
            item['Field13'] = ", ".join(images)

            item['Field14'] = response.url  # external_link

            agent1 = response.css(".mb-20")[0]
            agent1_name = agent1.css('.color-black .mb-0 ::text').get('').strip()
            if agent1:
                first_name1, last_name1 = agent1_name.split(maxsplit=1)
                item['Field15'] = first_name1.strip()  # agent_first_name_1
                item['Field16'] = last_name1.strip()  # agent_surname_name_1
                item['Field17'] = agent1_name.strip()  # agent_full_name_1
                item['Field18'] = agent1.css(".role .color-black ::text").get('').strip()
                agent1_phone = agent1.css(".phone .color-black ::text").getall()
                item['Field20'] = ' '.join(element.strip() for element in agent1_phone)
                agent1_mobile = agent1.css(".phone .color-black ::text").getall()
                item['Field21'] = ' '.join(element.strip() for element in agent1_mobile)
                '''         Uploading Images on Wasabi S3 Bucket            '''
                Agent_Imagess = agent1.css(".img-default-used-size-large ::attr(src)").getall()
                Agent_Imagesss = []
                for url in Agent_Imagess:
                    Agent_Imagesss.append(url)
                Agent_Images = ','.join(Agent_Imagesss[:1])
                new_name = agent1_name.replace(' ', '')
                id = agent1_phone[0].strip().replace(' ', '')
                # item['Field22'] = Agent_Images   # listing_images
                # print('Agent1 :',Agent_Images,',', new_name,',', id)
                images = self.uploaded(Agent_Images, new_name, id)  ## ','saperated Images(string), agent_name, agent_id
                item['Field22'] = ", ".join(images)

            if len(response.css(".mb-20").getall()) > 1:
                agent2 = response.css(".mb-20")[1]
                agent2_name = agent2.css('.color-black .mb-0 ::text').get('').strip()
                if agent2:
                    item['Field23'] = agent2_name.strip()
                    item['Field24'] = agent2.css(".role .color-black ::text").get('').strip()
                    agent2_phone = agent2.css(".phone .color-black ::text").getall()
                    item['Field26'] = ' '.join(element.strip() for element in agent2_phone)
                    agent2_mobile = agent2.css(".phone .color-black ::text").getall()
                    item['Field26A'] = ' '.join(element.strip() for element in agent2_mobile)

                    '''         Uploading Images on Wasabi S3 Bucket            '''
                    Agent_Imagess = agent2.css(".img-default-used-size-large ::attr(src)").getall()
                    Agent_Imagesss = []
                    for url in Agent_Imagess:
                        Agent_Imagesss.append(url)
                    Agent_Images = ','.join(Agent_Imagesss[:1])
                    new_name = agent2_name.replace(' ', '')
                    id = agent2_phone[0].strip().replace(' ', '')
                    # item['Field27'] = Agent_Images
                    # print('Agent2 :',Agent_Images,',', new_name,',', id)
                    images = self.uploaded(Agent_Images, new_name, id)  ## ','saperated Images(string), agent_name, agent_id
                    item['Field27'] = ", ".join(images)

            item['Field33'] = response.xpath("//*[contains(text(),'Property ID')]/following-sibling::div[1]/text()").get('').strip().replace(': ','')   # external_property_id
            item['Field35'] = response.xpath("//*[contains(text(),'Type')]/following-sibling::div[1]/text()").get('').strip().replace(': ','')   # external_property_id

            land_area = response.xpath("//*[contains(text(),'Building Size')]/following-sibling::div[1]/text()").get('').strip().replace(': ','')   # external_property_id
            if land_area:
                item['Field36'] = land_area.replace('sqm','').replace(' ','')   # land_area

            feature_count = 58
            for feature in response.css(".col-sm-6"):
                if 'Air Conditioning' in feature.css('::text').get('').strip():
                    item['Field52'] = feature.css("::text").get('').strip()
                else:
                    item[f'Field{feature_count}'] = feature.css("::text").get('').strip()
                    feature_count += 1

            print(item)
            self.insert_database(item)
        else:
            print('Data already exists')

    def download_image(self, img_url, file_dir, name):
        try:
            response = requests.request(method='GET', url=img_url)
            if response.status_code == 200:
                if not os.path.exists(file_dir):
                    os.makedirs(file_dir)
                with open(os.path.join(file_dir, name), 'wb') as file:
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
            self.download_image(image_url, local_file_path, title_name)
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
