########    Mint_Rent
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
class Mint_Rent(scrapy.Spider):
    name = 'Mint_Rent'
    prefix = 'https://www.mintpropertyagents.com.au'
    url = "https://www.mintpropertyagents.com.au/browse-rentals"
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9,ur;q=0.8,nl;q=0.7',
        'cache-control': 'max-age=0',
        # 'cookie': '_gid=GA1.3.2061402656.1711706631; _ga_B9CL2R23SX=GS1.1.1711706630.1.1.1711706755.0.0.0; _ga=GA1.1.535125209.1711706630; _eagle_session=VnlFeEtuemthUVdLZWJXaHpTRDN3V1I1MllkUGJVMVJYbTRuRnVIZ3RpaWdKd09saVVXai84UUlWaFMwU3JVY1hza1YzTmt5SjJTMFg5K0wvQ01EZkRPdXY4cWdGWFhpcTZYcGhPajdxbXg3MTdhNmY2c0tjbUxXVHVTbnQweVFTRC9Na0VlRll6aFBXcWl2ZVR4OC8yZWF3WG51QmJicjMydGwzK0lQRG51cExRWnY2NFBqTHAxZWtxZ1dLVGNvLS14aUo0ajlCQWs0SG5yVUMwcHV5Y2ZnPT0%3D--7cbc0e84bd8cb0aa55cad7eb4e23d58f0f4c881c; arp_scroll_position=140; _eagle_session=YTkrc2x6UmlyaC9CQlp6RkZneUlWdUZ6UFNGTkt1RlBnYys4WUxEWkthNFd2QU13Q0RyYmxqN01ZYXZrUnBkdjZxYkNlUWhpVG80M1ROUjNJZ0VwRnpTTHNNZTdKNmJsdFA1QVZTVXhtRGZvMDRtZ29wcG1KY1lxU3V1U1ErS252OENXZlV0QVYvaG8veXJPYnh3dmlDdXhoUUpqR2w4bDJRQmZHZkdnRzVBeUk3T2QyY0pJd1FKaDBHb0lwZXVILS1jNzBuK0dxWXZYcktmS1Y2b1NIbVlnPT0%3D--0f8808a3329ad1b1d7cc2c80219507e77abf8d1e',
        # 'referer': 'https://www.mintpropertyagents.com.au/commercial',
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
    db = 'Mint'
    collection = 'Mint_Rent'
    bucket_prefix = f'D_{collection}'

    local_file_path = '/img'


    def start_requests(self):
        yield scrapy.Request(url=self.url, headers=self.headers, callback=self.parse)
        yield scrapy.Request(url='https://www.mintpropertyagents.com.au/commercial-leases', headers=self.headers, callback=self.parse)

    def parse(self, response):
        for property in response.css(".property_catalog .property_thumbnail"):#[:1]:
            property_url = self.prefix + property.css('a ::attr(href)').get('').strip()
            print(self.count, property_url)
            self.count += 1
            yield scrapy.Request(url=property_url, headers=self.headers, callback=self.Detail_parse)

    def Detail_parse(self, response):
        DB_already_exists = self.read_data_base(response.url)
        if not DB_already_exists:
            item = dict()
            item['Field2'] = '1136'
            item['Field3'] = datetime.now().date().strftime('%Y-%m-%d')  # date_data_scanned
            property_add = response.css('.service_title ::text').getall()  # full_adddress
            property_address = ' '.join(element.strip().replace('\t', ' ') for element in property_add)  # description

            item['Field5'] = property_address.strip()  # full_adddress

            item['Field6'] = response.xpath("//span[contains(text(),' bedrooms')]/text()").get('').strip().replace(' bedrooms','')
            item['Field7'] = response.xpath("//span[contains(text(),' bathrooms')]/text()").get('').strip().replace(' bathrooms','')
            item['Field8'] = response.xpath("//span[contains(text(),' car parking')]/text()").get('').strip().replace(' car parking','')

            # sold_price = response.xpath("//*[contains(@data-testid,'listing-price')]/text()").get('').strip()
            # if sold_price:
            #     item['Field9'] = (sold_price.strip()).replace('$','')   # sold_price

            description = response.css('.property_content_description p ::text').getall()  # description
            item['Field12'] = ' '.join(element.strip().replace('\t', ' ') for element in description)  # description

            '''         Uploading Images on Wasabi S3 Bucket            '''
            Agent_Imagess = response.css('#Q98SEG1WYV img::attr(src)').getall()
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

            agent1 = response.css(".agent_small")
            agent1_name = agent1.css('.name a ::text').get('').strip()
            if agent1:
                first_name1, last_name1 = agent1_name.split(maxsplit=1)
                item['Field15'] = first_name1.strip()  # agent_first_name_1
                item['Field16'] = last_name1.strip()  # agent_surname_name_1
                item['Field17'] = agent1_name.strip()  # agent_full_name_1
                agent1_phone = agent1.xpath(".//a[contains(@href,'tel:')]/text()").getall()
                item['Field20'] = agent1_phone[0].strip()
                '''         Uploading Images on Wasabi S3 Bucket            '''
                Agent_Imagess = agent1.css(".rounded-circle ::attr(src)").getall()
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

            if len(response.css(".agent_small").getall()) > 1:
                agent2 = response.css(".agent_small")[1]
                agent2_name = agent2.css('.name a ::text').get('').strip()
                if agent2:
                    item['Field23'] = agent2_name.strip()
                    agent2_phone = agent2.xpath(".//a[contains(@href,'tel:')]/text()").getall()
                    item['Field26'] = agent2_phone[0].strip()
                    '''         Uploading Images on Wasabi S3 Bucket            '''
                    Agent_Imagess = agent2.css(".rounded-circle ::attr(src)").getall()
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

            # # item['Field35'] = response.xpath("//*[contains(text(),'Property Type')]/parent::li/text()").get('').strip().replace(': ','')   # external_property_id

            land_area = response.xpath("//sup[contains(text(),'2')]/parent::span/text()").get('').strip()
            if land_area:
                item['Field36'] = land_area.replace('m','').replace(' ','')   # land_area

            feature_count = 58
            for feature in response.css(".single-feature a"):
                if feature.css('::text') == 'Air Conditioning':
                    item['Field52'] = feature.css("::text").get('').strip()
                else:
                    item[f'Field{feature_count}'] = feature.css("::text").get('').strip()
                    feature_count += 1

            # print(item)
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




