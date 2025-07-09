######  Shores_Sale

import re
import csv
import copy
import scrapy
import pymongo
import mimetypes
from parsel import Selector
from datetime import datetime
import requests,json,time,threading,queue,os
import boto3
from botocore.exceptions import NoCredentialsError
from botocore.exceptions import NoCredentialsError, ClientError

class Shores_Sale(scrapy.Spider):
    name = 'Shores_Sale'
    prefix = 'https://www.shores.com.au'
    url = "https://www.shores.com.au/buy/properties-for-sale/"
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'en-US,en;q=0.9,ur;q=0.8,nl;q=0.7',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        # 'Cookie': '_ga=GA1.1.2056493952.1710930594; _ga_DN77FWQVSR=GS1.1.1710930593.1.1.1710930599.0.0.0',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"'
    }
    # custom_settings = {
    #     'FEED_URI': 'Output/Shores - Sale.csv',
    #     'FEED_FORMAT': 'csv',
    #     'FEED_EXPORT_ENCODING': 'utf-8-sig',
    # }

    def start_requests(self):
        yield scrapy.Request(url=self.url, headers=self.headers, callback=self.parse)

    def parse(self, response):
        for property in response.xpath("//*[contains(@class,'listing-item position-relative px-3')]"):#[:2]:
            property_url = property.css('a ::attr(href)').get('').strip()
            yield scrapy.Request(url=property_url, headers=self.headers, callback=self.Detail_parse)

        '''    PAGINATION    '''
        if response.css('.next'):
            next_page_url = response.css('.next ::attr(href)').get('').strip()
            yield scrapy.Request(url=next_page_url, headers=self.headers, callback=self.parse)

    def Detail_parse(self, response):
        DB_already_exists = self.read_data_base(response.url)
        if not DB_already_exists:
            item = dict()
            item['Field2'] = '1027'
            item['Field3'] = datetime.now().date().strftime('%Y-%m-%d')   # date_data_scanned
            item['Field5'] = response.css('.property-address ::text').get('').strip()   # full_adddress

            bed = response.css('.bed ::text').getall()
            if bed:
                item['Field6'] = bed[-1].strip()   # bedrooms
            bath = response.css('.bath ::text').getall()
            if bath:
                item['Field7'] = bath[-1].strip()   # bedrooms
            car = response.css('.car ::text').getall()
            if car:
                item['Field8'] = car[-1].strip()   # bedrooms

            sold_price = response.css('.property-price ::text').get('').strip()   # sold_price
            price_pattern = r'\$(\d{1,3}(?:,\d{3})*)'
            price_match = re.search(price_pattern, sold_price)
            if price_match:
                price_str = price_match.group(1).replace(',', '')  # Remove comma before converting to int
                price = int(price_str)
                item['Field9'] = price   # sold_price
            else:
                item['Field9'] = ''   # sold_price
            description = response.css('.detail-description span::text').getall()   # description
            item['Field12'] = ', '.join(element.strip() for element in description)   # description
            new_img_urls = response.css('.img-default-used-size-medium ::attr(src)').getall()

            '''         Uploading Images on Wasabi S3 Bucket            '''
            Images = ','.join(new_img_urls)
            aname = response.css('.property-address ::text').get('').strip()
            new_name = aname.replace(' ','')
            id= (response.url).split('/')[-1]
            # for wasabi_cloud
            images = self.uploaded(Images, new_name,id)
            total_images=  ", ".join(images)
            item['Field13'] = total_images   # listing_images

            item['Field14'] = response.url   # external_link
            agent1_name = response.css('.color-black .mb-0 ::text').get('').strip()
            first_name1, last_name1 = agent1_name.split(maxsplit=1)
            item['Field15'] = first_name1.strip()   # agent_first_name_1
            item['Field16'] = last_name1.strip()   # agent_surname_name_1
            item['Field17'] = agent1_name.strip()   # agent_full_name_1
            item['Field18'] = ''   # agent_title_1
            item['Field19'] = (response.css('.email .color-black ::attr(href)').get('').strip()).replace('mailto:','')   # agent_email_1
            item['Field20'] = response.css('.mobile .color-black ::text').get('').strip()   # agent_mobile_1
            item['Field21'] = response.css('.phone .color-black ::text').get('').strip()   # agent_phnumber_1
            '''         Uploading Images on Wasabi S3 Bucket            '''
            # item['Field22'] = response.css('.mb-20 .img-default-used-size-large ::attr(src)').get('').strip()   # agent_image_1 ON WASABI $$$$$$$$$$$
            agent1_image = response.css('.mb-20 .img-default-used-size-large ::attr(src)').getall()   # agent_image_1 ON WASABI $$$$$$$$$$$
            Agent_Images = ','.join(agent1_image)
            aname = agent1_name.strip()
            new_name = aname.replace(' ','')
            agent1_id = response.xpath("//*[contains(@class,'d-block flex-grow-1 color-black')]/@href").get('').strip()
            id= (agent1_id).split('/')[-1]
            # for wasabi_cloud
            images = self.uploaded(Agent_Images, new_name, id)
            agent_image =  ", ".join(images)
            item['Field22'] = agent_image   # listing_images

            if len(response.css('.color-black .mb-0 ::text').getall()) == 2:
                agent2 = response.xpath("//*[contains(@class,'text-center flex-column d-flex flex-grow-1 col mb-20')][2]")   # agent_full_name_2
                item['Field23'] = agent2.css('.color-black .mb-0 ::text').get('').strip()   # agent_full_name_2
                item['Field24'] = ''   # agent_title_2
                item['Field25'] = (agent2.css('.email .color-black ::attr(href)').get('').strip()).replace('mailto:','')      # agent_email_2
                item['Field26'] = agent2.css('.mobile .color-black ::text').get('').strip()   # agent_mobile_2
                item['Field27'] = agent2.css('.img-default-used-size-large ::attr(src)').get('').strip()   # agent_image_2
            if len(response.css('.color-black .mb-0 ::text').getall()) == 3:
                agent3 = response.xpath("//*[contains(@class,'text-center flex-column d-flex flex-grow-1 col mb-20')][3]")
                item['Field28'] = agent3.css('.color-black .mb-0 ::text').get('').strip()   # agent_full_name_3
                item['Field29'] = ''   # agent_title_3
                item['Field30'] = (agent3.css('.email .color-black ::attr(href)').get('').strip()).replace('mailto:','')      # agent_email_3
                item['Field31'] = agent3.css('.mobile .color-black ::text').get('').strip()   # agent_mobile_3
                item['Field32'] = agent3.css('.img-default-used-size-large ::attr(src)').get('').strip()   # agent_image_3
            land_area = (response.xpath('//*[contains(text(),"Land size: ")]/text()').get('').strip()).replace('Land size: ','').replace(' sqm','')   # land_area
            if land_area:
                item['Field36'] = int(land_area)   # land_area
            item['Field102'] = response.css('.property-contract-label ::text').get('').strip()   # Status
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

    def uploaded(self, list_of_img, names,id):
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
        bucket_prefix = 'D_shores_sale'
        bucket_number = 1
        current_bucket_name = f'{bucket_prefix}_{bucket_number}'

        # existing_buckets = [bucket['Name'] for bucket in s3.list_buckets()['Buckets']]
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

    def read_data_base(self,profileUrl):
        url=profileUrl
        connection_string = 'mongodb://localhost:27017'
        conn = pymongo.MongoClient(connection_string)
        db = conn['Shores_Record']
        collection = db['Shores_Sale']
        # search_query = {"profileUrl": url}
        search_query = {"Field14": url}

        sale_urls_list_of_DB = []
        # all_matching_data = collection.find(search_query)
        all_matching_data = collection.find_one(search_query)

        if all_matching_data:
            print(all_matching_data.get('Field5'))
            return True
        else:
            return False

    def insert_database(self, new_data):
        connection_string = 'mongodb://localhost:27017'
        conn = pymongo.MongoClient(connection_string)
        db = conn['Shores_Record']
        collection = db['Shores_Sale']

        collection.insert_one(new_data)
        print("Data inserted successfully!")

    '''     Updated_Database Method     '''
    # def update_database(self, profileUrl, new_data):
    #     connection_string = 'mongodb://localhost:27017'
    #     conn = pymongo.MongoClient(connection_string)
    #     db = conn['Shores_Record']
    #     collection = db['Shores_Sale']
    #
    #     search_query = {"Field104": profileUrl}
    #     # print(search_query)
    #     update_query = {
    #         "$set": {
    #
    #             "Field102": new_data["Field102"],
    #             "Field103": new_data["Field103"]
    #         }
    #     }
    #     collection.update_one(search_query, update_query, upsert=True)
    #     print("Data updated successfully!")
