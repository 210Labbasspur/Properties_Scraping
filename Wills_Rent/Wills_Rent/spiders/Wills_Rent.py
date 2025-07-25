######  Wills_Rent
import re
import csv
import copy
import scrapy
import pymongo
import mimetypes
from parsel import Selector
from datetime import datetime
import requests, json, time, threading, queue, os
import boto3
from botocore.exceptions import NoCredentialsError
from botocore.exceptions import NoCredentialsError, ClientError
class Wills_Rent(scrapy.Spider):
    name = 'Wills_Rent'
    prefix = 'https://www.willsproperty.com.au'
    url = "https://www.willsproperty.com.au/lease/"
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9,ur;q=0.8,nl;q=0.7',
        'cache-control': 'max-age=0',
        # 'cookie': 'AMP_MKTG_16a5c84b5b=JTdCJTdE; _gid=GA1.3.1171844160.1711823825; _gcl_au=1.1.1561480658.1711823825; arp_scroll_position=742; AMP_16a5c84b5b=JTdCJTIyZGV2aWNlSWQlMjIlM0ElMjI4NjMzNmJhYy1lZThjLTQxMjAtODI0OC00NDA3MWMzMDhlYjElMjIlMkMlMjJzZXNzaW9uSWQlMjIlM0ExNzExODIzODI0NzkyJTJDJTIyb3B0T3V0JTIyJTNBZmFsc2UlMkMlMjJsYXN0RXZlbnRUaW1lJTIyJTNBMTcxMTgyNDc3MjU3MiU3RA==; _gat_gtag_UA_87981431_1=1; _ga_8R27K574X0=GS1.1.1711823826.1.1.1711824773.0.0.0; _ga=GA1.1.427375367.1711823825; _ga_QHL3S2XHQS=GS1.1.1711823825.1.1.1711824773.0.0.0',
        # 'if-none-match': '"906dfcc3ca2c608a16584feff851fdb8-ssl-df"',
        # 'referer': 'https://www.willsproperty.com.au/sold/',
        'sec-ch-ua': '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36'
    }

    count = 1
    db = 'Wills'
    collection = 'Wills_Rent'
    bucket_prefix = f'D_{collection}'

    local_file_path = '/img'


    def start_requests(self):
        yield scrapy.Request(url=self.url, headers=self.headers, callback=self.parse)

    def parse(self, response):
        for property in response.css(".properties__items .properties__item a")[:2]:
            property_url = self.prefix + property.css('::attr(href)').get('').strip()
            print(self.count, property_url)
            self.count += 1
            yield scrapy.Request(url=property_url, headers=self.headers, callback=self.Detail_parse)

    def Detail_parse(self, response):
        DB_already_exists = self.read_data_base(response.url)
        if not DB_already_exists:
            item = dict()
            item['Field2'] = '1154'
            item['Field3'] = datetime.now().date().strftime('%Y-%m-%d')  # date_data_scanned

            property_add = response.css('.single-property__main-title h1 ::text').getall()  # full_adddress
            property_address = ' '.join(element.strip().replace('\t', ' ') for element in property_add)  # description
            item['Field5'] = property_address.strip()  # full_adddress

            item['Field6'] = response.css('.single-property__specs li:nth-child(1) ::text').get('').strip()
            item['Field7'] = response.css('.single-property__specs li:nth-child(2) ::text').get('').strip()
            item['Field8'] = response.css('.single-property__specs li:nth-child(3) ::text').get('').strip()

            # sold_price = response.xpath("//*[contains(@data-testid,'listing-price')]/text()").get('').strip()
            # if sold_price:
            #     item['Field9'] = (sold_price.strip()).replace('$','')   # sold_price

            description = response.css('.single-property__content span ::text').getall()  # description
            item['Field12'] = ' '.join(element.strip().replace('\t', ' ') for element in description)  # description

            '''         Uploading Images on Wasabi S3 Bucket            '''
            Agent_Imagess = response.css('.fader__slide img ::attr(src)').getall()
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

            agent1 = response.xpath("//*[contains(@class,'single-property__staff')]/li")
            agent1_name = agent1.css('.name ::text').get('').strip()
            if agent1:
                first_name1, last_name1 = agent1_name.split(maxsplit=1)
                item['Field15'] = first_name1.strip()  # agent_first_name_1
                item['Field16'] = last_name1.strip()  # agent_surname_name_1
                item['Field17'] = agent1_name.strip()  # agent_full_name_1
                item['Field18'] = agent1.css('.single-property__agent-info ul li ::text').get('').strip()
                item['Field19'] = agent1.xpath(".//a[contains(@href,'mailto:')]/@href").get('').strip().replace('mailto:','')
                agent1_phone = agent1.xpath(".//a[contains(@href,'tel:')]/@href").get('').strip().replace('tel:','')
                item['Field20'] = agent1_phone.strip()
                '''         Uploading Images on Wasabi S3 Bucket            '''
                Agent_Imagess = agent1.css(".single-property__agent-thumb img ::attr(src)").getall()
                Agent_Imagesss = []
                for url in Agent_Imagess:
                    Agent_Imagesss.append(self.prefix + url)
                Agent_Images = ','.join(Agent_Imagesss[:1])
                new_name = agent1_name.replace(' ', '')
                id = agent1_phone.strip().replace(' ', '')
                # item['Field22'] = Agent_Images   # listing_images
                # print('Agent1 :',Agent_Images,',', new_name,',', id)
                images = self.uploaded(Agent_Images, new_name, id)  ## ','saperated Images(string), agent_name, agent_id
                item['Field22'] = ", ".join(images)

            if len(response.xpath("//*[contains(@class,'single-property__staff')]/li").getall()) > 1:
                agent2 = response.xpath("//*[contains(@class,'single-property__staff')]/li")[1]
                agent2_name = agent2.css('.name ::text').get('').strip()
                if agent2:
                    item['Field23'] = agent2_name.strip()
                    item['Field24'] = agent2.css('.single-property__agent-info ul li ::text').get('').strip()
                    item['Field25'] = agent2.xpath(".//a[contains(@href,'mailto:')]/@href").get('').strip().replace('mailto:', '')
                    agent2_phone = agent2.xpath(".//a[contains(@href,'tel:')]/@href").get('').strip().replace('tel:', '')
                    item['Field26'] = agent2_phone.strip()
                    '''         Uploading Images on Wasabi S3 Bucket            '''
                    Agent_Imagess = agent2.css(".single-property__agent-thumb img ::attr(src)").getall()
                    Agent_Imagesss = []
                    for url in Agent_Imagess:
                        Agent_Imagesss.append(self.prefix + url)
                    Agent_Images = ','.join(Agent_Imagesss[:1])
                    new_name = agent2_name.replace(' ', '')
                    id = agent2_phone.strip().replace(' ', '')
                    # item['Field27'] = Agent_Images
                    # print('Agent2 :',Agent_Images,',', new_name,',', id)
                    images = self.uploaded(Agent_Images, new_name, id)  ## ','saperated Images(string), agent_name, agent_id
                    item['Field27'] = ", ".join(images)

            # # item['Field35'] = response.css('.property-category ::text').get('').strip()
            # #
            # # land_area = response.css('.land-size ::text').get('').strip()
            # # if land_area:
            # #     item['Field36'] = land_area.replace('Land is','').replace('m²','').replace(' ','')   # land_area
            # #
            # # feature_count = 58
            # # for feature in response.css(".epl-tax-feature"):
            # #     item[f'Field{feature_count}'] = feature.css(".epl-tax-feature a ::text").get('').strip()
            # #     feature_count += 1

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

        wasabi_access_key = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
        wasabi_secret_key = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
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

            local_file_path = self.local_file_path

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
