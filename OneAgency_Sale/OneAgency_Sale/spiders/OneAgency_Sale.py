#####       OneAgency_Sale

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
class OneAgency_Sale(scrapy.Spider):
    name = 'OneAgency_Sale'
    prefix = 'https://oneagencyforest.com.au'
    url = "https://oneagencyforest.com.au/listings?saleOrRental=Sale&status=available_under_contract&sortby=dateListed-desc"
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9,ur;q=0.8,nl;q=0.7',
        'cache-control': 'max-age=0',
        # 'cookie': 'arp_scroll_position=176',
        'sec-ch-ua': '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'none',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36'
    }

    count = 1
    db = 'OneAgency'
    collection = 'OneAgency_Sale'
    bucket_prefix = 'D_OneAgency_Sale'

    local_file_path = '/img'


    def start_requests(self):
        yield scrapy.Request(url=self.url, headers=self.headers, callback=self.parse)

    def parse(self, response):
        for property in response.xpath("//*[contains(@class,'v2-flex v2-flex-col v2-relative v2-gap-y-3')]"):#[:2]:
            property_url = self.prefix + property.css('a ::attr(href)').get('').strip()
            print(self.count, property_url)
            self.count += 1
            yield scrapy.Request(url=property_url, headers=self.headers, callback=self.Detail_parse)

        '''    PAGINATION    '''
        if response.xpath("//a[contains(text(),'Next')]"):
            next_page_url = self.prefix + response.xpath("//a[contains(text(),'Next')]/@href").get('').strip()
            yield scrapy.Request(url=next_page_url, headers=self.headers, callback=self.parse)


    def Detail_parse(self, response):
        DB_already_exists = self.read_data_base(response.url)
        if not DB_already_exists:
            item = dict()
            item['Field2'] = '1116'
            item['Field3'] = datetime.now().date().strftime('%Y-%m-%d')   # date_data_scanned

            property_address = response.xpath("//*[contains(@data-testid,'address')]/text()").get('').strip()   # full_adddress
            item['Field5'] = property_address.strip()   # full_adddress

            bed = response.xpath("//*[contains(@data-testid,'stats-bed')]/p/text()").get('').strip()
            if bed:
                item['Field6'] = bed.strip().replace(' Bed','')   # bedrooms
            bath = response.xpath("//*[contains(@data-testid,'stats-bath')]/p/text()").get('').strip()
            if bath:
                item['Field7'] = bath.strip().replace(' Bath','')   # bedrooms
            car = response.xpath("//*[contains(@data-testid,'stats-car')]/p/text()").get('').strip()
            if car:
                item['Field8'] = car.strip().replace(' Car','')   # bedrooms

            # sold_price = response.xpath("//*[contains(@data-testid,'listing-price')]/text()").get('').strip()
            # if sold_price:
            #     item['Field9'] = (sold_price.strip()).replace('$','')   # sold_price

            description = response.css('.siteloft-ucg p::text').getall()   # description
            item['Field12'] = ', '.join(element.strip().replace('\t',' ') for element in description)   # description
            '''         Uploading Images on Wasabi S3 Bucket            '''
            images_in_script = response.xpath("//div[contains(@id,'shared/components/listing-gallery/ListingGallery')]/script/text()").get('').strip()
            img_urls = response.xpath("//*[contains(@class,'v2-hidden lg:v2-block lg:v2-absolute lg:v2-w-full lg:v2-h-full lg:v2-object-cover')]/@src").getall()
            new_img_urls = []
            for url in img_urls:
                new_img_urls.append('https:' + url)
            # item['Field13'] = new_img_urls   # listing_images
            Images = ','.join(new_img_urls)
            new_name = property_address.replace(' ','').replace('/','_')
            id = (response.url).split('/')[-1]
            print(Images,new_name,id)
            images = self.uploaded(Images,new_name,id)       ## ','saperated Images(string), Property_address, URL_id
            item['Field13'] = ", ".join(images)

            item['Field14'] = response.url   # external_link

            agent1 = response.xpath("//*[contains(@data-testid,'agent-details')]/div[contains(@class,'v2-flex v2-gap-5 v2-items-center')][1]")
            agent1_name = agent1.css('.v2-capitalize.hover\:v2-underline ::text').get('').strip()
            if agent1_name:
                first_name1, last_name1 = agent1_name.split(maxsplit=1)
                item['Field15'] = first_name1.strip()   # agent_first_name_1
                item['Field16'] = last_name1.strip()   # agent_surname_name_1
                item['Field17'] = agent1_name.strip()   # agent_full_name_1
                item['Field18'] = agent1.css('.v2-text-text-grey.v2-capitalize ::text').get('').strip()
                item['Field19'] = agent1.css('.hover\:v2-text-dark-hover+ .hover\:v2-text-dark-hover .hover\:v2-underline ::text').get('').strip()
                item['Field20'] = agent1.css('.hover\:v2-text-dark-hover:nth-child(1) .hover\:v2-underline ::text').get('').strip()
                '''         Uploading Images on Wasabi S3 Bucket            '''
                Agent_Imagesss = agent1.css('.v2-items-center:nth-child(1) .v2-h-full ::attr(src)').getall()
                Agent_Images = ','.join(Agent_Imagesss[:1])
                # item['Field22'] = Agent_Images   # listing_images
                new_name = agent1_name.replace(' ','')
                agent1_id = agent1.css('.v2-capitalize.hover\:v2-underline ::attr(href)').get('').strip()
                id = (agent1_id).split('/')[-1]
                # print(Agent_Images, new_name, id)
                images = self.uploaded(Agent_Images, new_name, id)        ## ','saperated Images(string), agent_name, agent_id
                item['Field22'] = ", ".join(images)

            agent2 = response.xpath("//*[contains(@data-testid,'agent-details')]/div[contains(@class,'v2-flex v2-gap-5 v2-items-center')][2]")
            if agent2:
                item['Field23'] = agent2.css('.v2-capitalize.hover\:v2-underline ::text').get('').strip()
                item['Field24'] = agent2.css('.v2-text-text-grey.v2-capitalize ::text').get('').strip()
                item['Field25'] = agent2.css('.hover\:v2-text-dark-hover+ .hover\:v2-text-dark-hover .hover\:v2-underline ::text').get('').strip()
                item['Field26'] = agent2.css('.hover\:v2-text-dark-hover:nth-child(1) .hover\:v2-underline ::text').get('').strip()

                '''         Uploading Images on Wasabi S3 Bucket            '''
                Agent_Imagesss = agent2.css('.v2-items-center:nth-child(2) .v2-h-full ::attr(src)').getall()
                Agent_Images = ','.join(Agent_Imagesss[:1])
                # item['Field27'] = Agent_Images
                new_name = (agent2.css('.v2-capitalize.hover\:v2-underline ::text').get('').strip()).replace(' ','')
                agent2_id = agent2.css('.v2-capitalize.hover\:v2-underline ::attr(href)').get('').strip()
                id = (agent2_id).split('/')[-1]
                print(Agent_Images, new_name, id)
                images = self.uploaded(Agent_Images, new_name, id)        ## ','saperated Images(string), agent_name, agent_id
                item['Field27'] = ", ".join(images)

            agent3 = response.xpath("//*[contains(@class,'bg-black-5 p-3 d-flex flex-grow-1 flex-column')][3]")
            if agent3:
                item['Field28'] = agent3.css('.v2-capitalize.hover\:v2-underline ::text').get('').strip()
                item['Field29'] = agent3.css('.v2-text-text-grey.v2-capitalize ::text').get('').strip()
                item['Field30'] = agent3.css('.hover\:v2-text-dark-hover+ .hover\:v2-text-dark-hover .hover\:v2-underline ::text').get('').strip()
                item['Field31'] = agent3.css('.hover\:v2-text-dark-hover:nth-child(1) .hover\:v2-underline ::text').get('').strip()

                '''         Uploading Images on Wasabi S3 Bucket            '''
                Agent_Imagesss = agent3.css('.v2-items-center:nth-child(3) .v2-h-full ::attr(src)').getall()
                Agent_Images = ','.join(Agent_Imagesss[:1])
                # item['Field32'] = Agent_Images
                new_name = (agent3.css('.v2-capitalize.hover\:v2-underline ::text').get('').strip()).replace(' ','')
                agent3_id = agent3.css('.v2-capitalize.hover\:v2-underline ::attr(href)').get('').strip()
                id = (agent3_id).split('/')[-1]
                # print(Agent_Images, new_name, id)
                images = self.uploaded(Agent_Images, new_name, id)        ## ','saperated Images(string), agent_name, agent_id
                item['Field32'] = ", ".join(images)

            item['Field33'] = response.xpath("//span[contains(text(),'Property ID')]/following-sibling::span/text()").get('').strip()   # external_property_id
            # item['Field35'] = response.xpath("//label[contains(text(),'Type')]/following-sibling::div/text()").get('').strip()   # external_property_id

            land_area = response.xpath("//span[contains(text(),'Land size')]/following-sibling::span/text()").get('').strip()   # external_property_id
            if land_area:
                item['Field36'] = land_area.replace(' m²','')   # land_area

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

    def uploaded(self, list_of_img, names,id):
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

    def read_data_base(self,profileUrl):
        url=profileUrl
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




