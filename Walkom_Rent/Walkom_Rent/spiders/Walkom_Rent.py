##################      Walkom_Rent

import re
from copy import deepcopy

import scrapy
import pymongo
import mimetypes
from datetime import datetime
import requests, json, time, threading, queue, os
import boto3
from botocore.exceptions import NoCredentialsError, ClientError

class Walkom_Rent(scrapy.Spider):
    name = 'Walkom_Rent'
    prefix = 'https://www.walkom.com.au'
    url = 'https://www.walkom.com.au/wp-content/plugins/zoorealty/display/pages/ajax/fetch_pages.php'
    headers = {
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.9,ur;q=0.8,nl;q=0.7',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        # 'cookie': '_gcl_au=1.1.6293168.1714476011; __utmz=212302226.1714476015.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); hubspotutk=251eef386b668a360df2970b703d6c77; __utmc=212302226; _gid=GA1.3.427206677.1714693143; __hssrc=1; PHPSESSID=2j60fcj280uoca5pivolj31b1m; _ga_T2TEM6Y0QX=GS1.1.1714783472.5.0.1714783472.0.0.0; __utma=212302226.437115297.1714476010.1714735459.1714783476.4; __utmt=1; __utmb=212302226.1.10.1714783476; _ga_KZ9JN79M9Z=GS1.1.1714783476.5.0.1714783476.0.0.0; _ga=GA1.3.437115297.1714476010; _gat_gtag_UA_101714508_1=1; __hstc=166323425.251eef386b668a360df2970b703d6c77.1714476015270.1714735460527.1714783477311.4; __hssc=166323425.1.1714783477311; arp_scroll_position=1839',
        'origin': 'https://www.walkom.com.au',
        'priority': 'u=1, i',
        # 'referer': 'https://www.walkom.com.au/selling/recent-sales/',
        'sec-ch-ua': '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        'x-requested-with': 'XMLHttpRequest',
    }

    data = {
        'page': '0',
        'dlist': 'lease',
        'dsuburb': '',
        'office_id': '',
        'keywords': '',
        'price_min': '',
        'price_max': '',
        'bedrooms': '',
        'bathrooms': '',
        'carspaces': '',
        'status': '1,4',
        'order': 'created_at',
        # 'order': 'field(properties.id,1902402,3658072) desc, new desc, properties.created_at',
        'order_direction': 'desc',
        # 'exclude_id': '',
        # 'include_non_commercial_lease_id': '',
        # 'days_limit': '',
    }
    count = 1
    db = 'Walkom'
    collection = 'Walkom_Rent'
    bucket_prefix = f'D_{collection}'

    def start_requests(self):
        property_no = 0
        page_no = 0
        payload = deepcopy(self.data)
        payload['page'] = str(property_no)
        yield scrapy.FormRequest(url=self.url, formdata=payload, method='POST', callback=self.parse, headers=self.headers,
                             meta={'page_no':page_no,'property_no': property_no})

    def parse(self, response):
        property_no = response.meta['property_no']
        for property in response.xpath("//*[contains(@class,'items owl-carousel')]/div[1]/"
                                       "a[contains(@class,'placeholder')]"):
            property_no += 1
            property_url = property.css('::attr(href)').get('').strip()
            print(self.count, property_url)
            self.count += 1
            yield response.follow(url=property_url, headers=self.headers, callback=self.Detail_parse)

        if response.css('.owl-carousel'):
            page_no = response.meta['page_no'] + 1
            payload = deepcopy(self.data)
            payload['page'] = str(page_no)
            yield scrapy.FormRequest(url=self.url, formdata=payload, method='POST', callback=self.parse, headers=self.headers,
                                     meta={'page_no': page_no, 'property_no': property_no})

    def Detail_parse(self, response):
        DB_already_exists = self.read_data_base(response.url)
        if not DB_already_exists:
            item = dict()
            item['Field2'] = '1360'
            item['Field3'] = datetime.now().date().strftime('%Y-%m-%d')  # date_data_scanned
            item['Field4'] = 'Walkom'

            property_add = response.css('.address ::text').getall()  # full_adddress
            property_address = ' '.join(element.strip().replace('\n', ' ').replace('\t','') for element in property_add)  # description
            item['Field5'] = property_address.strip()  # full_adddress

            item['Field6'] = response.xpath("//*[contains(@class,'fa fa-bed')]/parent::li[1]/text()").get('').strip()
            item['Field7'] = response.xpath("//*[contains(@class,'fa fa-bath')]/parent::li[1]/text()").get('').strip()
            item['Field8'] = response.xpath("//*[contains(@class,'fa fa-car')]/parent::li[1]/text()").get('').strip()

            # sold_price = response.xpath("//*[contains(text(),'SOLD')]/text()").get('').strip().replace('SOLD','').replace(' ','')
            # if re.search(r'\d', sold_price):
            #     item['Field9'] = (sold_price.strip()).replace('$','')   # sold_price

            description = response.css('#less_property_description ::text').getall()  # description
            item['Field12'] = ' '.join(element.strip().replace('\t', ' ').replace('[More]', ' ') for element in description)  # description

            '''         Uploading Images on Wasabi S3 Bucket            '''
            Agent_Imagess = response.xpath("//*[contains(@data-photourl,'https://img.agentaccount.com/')]/@data-photourl").getall()
            new_img_urls = []
            for url in Agent_Imagess:
                new_img_urls.append(url)
            Images = ', '.join(new_img_urls)
            new_name = self.db.replace(' ', '').replace('/', '_')
            id = (response.url).split('/')[-2]
            # item['Field13'] = new_img_urls   # listing_images
            # print(Images,new_name,id)
            images = self.uploaded(Images, new_name, id)  ## ','saperated Images(string), Property_address, URL_id
            item['Field13'] = ", ".join(images)

            item['Field14'] = response.url  # external_link
            '''          AGENTS          '''
            if response.css("#property-enquiry .agent"):
                agent1 = response.css("#property-enquiry .agent")[0]
                agent1_name = agent1.css('strong ::text').get('').strip()
                if agent1:
                    first_name1, last_name1 = agent1_name.split(maxsplit=1)
                    item['Field15'] = first_name1.strip()  # agent_first_name_1
                    item['Field16'] = last_name1.strip()  # agent_surname_name_1
                    item['Field17'] = agent1_name.strip()  # agent_full_name_1
                    item['Field18'] =  agent1.css(".mobile+ p ::text").get('').strip()
                    # item['Field19'] =  agent1.css(".contact ::text").get('').strip().replace('E:','')
                    agent1_phone = agent1.css(".mobile ::text").get('').strip()
                    item['Field20'] = agent1_phone.strip()
                    agent1_mobile = agent1.css(".phone ::text").get('').strip()
                    item['Field21'] = agent1_mobile.strip()
                    '''         Uploading Images on Wasabi S3 Bucket            '''
                    Agent_Images = agent1.css("img ::attr(src)").get('').strip()
                    new_name = agent1_name.replace(' ', '')
                    id = agent1_phone.strip().replace(' ', '')
                    # item['Field22'] = Agent_Images   # listing_images
                    # print('Agent1 :',Agent_Images,',', new_name,',', id)
                    images = self.uploaded(Agent_Images, new_name, id)  ## ','saperated Images(string), agent_name, agent_id
                    item['Field22'] = ", ".join(images)

                if len(response.css("#property-enquiry .agent").getall()) > 1:
                    agent2 = response.css("#property-enquiry .agent")[1]
                    agent2_name = agent2.css('strong ::text').get('').strip()
                    if agent2:
                        item['Field23'] = agent2_name.strip()
                        item['Field24'] = agent1.css(".mobile+ p ::text").get('').strip()
                        # item['Field25'] = agent2.css(".contact ::text").get('').strip().replace('E:', '')
                        agent2_phone = agent2.css(".mobile ::text").get('').strip()
                        item['Field26'] = agent2_phone.strip()
                        agent2_mobile = agent2.css(".phone ::text").get('').strip()
                        item['Field26A'] = agent2_mobile.strip()
                        '''         Uploading Images on Wasabi S3 Bucket            '''
                        Agent_Images = agent2.css("img ::attr(src)").get('').strip()
                        new_name = agent2_name.replace(' ', '')
                        id = agent2_phone.strip().replace(' ', '')
                        # item['Field27'] = Agent_Images
                        # print('Agent2 :',Agent_Images,',', new_name,',', id)
                        images = self.uploaded(Agent_Images, new_name, id)  ## ','saperated Images(string), agent_name, agent_id
                        item['Field27'] = ", ".join(images)

                if len(response.css("#property-enquiry .agent").getall()) > 2:
                    agent3 = response.css("#property-enquiry .agent")[2]
                    agent3_name = agent3.css('strong ::text').get('').strip()
                    if agent3:
                        item['Field28'] = agent3_name.strip()
                        item['Field29'] = agent1.css(".mobile+ p ::text").get('').strip()
                        # item['Field30'] = agent3.css(".contact ::text").get('').strip().replace('E:', '')
                        agent3_phone = agent3.css(".mobile ::text").get('').strip()
                        item['Field31'] = agent3_phone.strip()
                        agent3_mobile = agent3.css(".phone ::text").get('').strip()
                        item['Field31A'] = agent3_mobile.strip()
                        '''         Uploading Images on Wasabi S3 Bucket            '''
                        Agent_Images = agent3.css("img ::attr(src)").get('').strip()
                        new_name = agent3_name.replace(' ', '')
                        id = agent3_phone.strip().replace(' ', '')
                        # item['Field32'] = Agent_Images
                        # print('Agent3 :',Agent_Images,',', new_name,',', id)
                        images = self.uploaded(Agent_Images, new_name, id)  ## ','saperated Images(string), agent_name, agent_id
                        item['Field32'] = ", ".join(images)

            # item['Field33'] = response.xpath("//*[contains(text(),'Property ID')]/text()").get('').strip().replace('Property ID:','')
            item['Field35'] = response.css(".property_type .value ::text").get('').strip()
            # land_area = response.xpath("//*[contains(text(),'Land Size')]/following-sibling::div[1]/text()").get('').strip()
            # if land_area:
            #     item['Field36'] = land_area.replace('sqm','').replace(' ','')   # land_area
            # elif response.xpath("//*[contains(text(),'Building Size')]/following-sibling::div[1]/text()").get('').strip():
            #     item['Field36'] = land_area.replace('sqm','').replace(' ','')   # land_area

            feature_count = 58
            for feature in response.css(".block_content span"):
                if 'Air Condition' in feature.css('::text').get('').strip():
                    item['Field52'] = feature.css('::text').get('').strip()
                else:
                    item[f'Field{feature_count}'] = feature.css('::text').get('').strip()
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

        wasabi_access_key = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
        wasabi_secret_key = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
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
