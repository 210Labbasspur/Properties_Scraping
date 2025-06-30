########        DeansP_Rent
import re
import scrapy
import pymongo
import mimetypes
from datetime import datetime
import requests, json, time, threading, queue, os
import boto3
from botocore.exceptions import NoCredentialsError, ClientError

class DeansP_Rent(scrapy.Spider):
    name = 'DeansP_Rent'
    prefix = 'https://www.deansproperty.com.au'
    url = "https://www.deansproperty.com.au/for-lease/current-listings/"
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'en-US,en;q=0.9,ur;q=0.8,nl;q=0.7',
        'Connection': 'keep-alive',
        # 'Cookie': '_gcl_au=1.1.997717354.1711837752; PHPSESSID=il03s709ataruahdjvb9tkgalf; _ga_GZ0BW37P5R=GS1.1.1711987112.2.0.1711987112.0.0.0; _ga_NRV2B2V0W1=GS1.1.1711987112.2.0.1711987112.0.0.0; _ga_4VX3KZRH5Q=GS1.1.1711987117.2.0.1711987117.0.0.0; _ga_RFXEKFC7M0=GS1.1.1711987125.2.0.1711987125.0.0.0; _gid=GA1.3.1343719713.1711987126; _ga_L4PE4Z4FHD=GS1.1.1711987126.2.0.1711987126.0.0.0; _ga=GA1.1.503511561.1711837748; _ga_P4GZVQ5LYK=GS1.3.1711987126.2.0.1711987126.0.0.0; arp_scroll_position=100; PHPSESSID=il03s709ataruahdjvb9tkgalf',
        # 'Referer': 'https://www.deansproperty.com.au/for-sale/recent-sales/',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"'
    }

    count = 1
    db = 'DeansP'
    collection = 'DeansP_Rent'
    bucket_prefix = f'D_{collection}'

    local_file_path = '/img'


    def start_requests(self):
        yield scrapy.Request(url=self.url, headers=self.headers, callback=self.parse)

    def parse(self, response):
        for property in response.css('.address a'):#[:2]:
            property_url = property.css('::attr(href)').get('').strip()
            print(self.count, property_url)
            self.count += 1
            yield scrapy.Request(url=property_url, headers=self.headers, callback=self.Detail_parse)

        '''    PAGINATION    '''
        if response.css('.next'):
            next_page_url = self.prefix + response.css('.next ::attr(href)').get('').strip()
            yield scrapy.Request(url=next_page_url, headers=self.headers, callback=self.parse)

    def Detail_parse(self, response):
        DB_already_exists = self.read_data_base(response.url)
        if not DB_already_exists:
            item = dict()
            item['Field2'] = '1160'
            item['Field3'] = datetime.now().date().strftime('%Y-%m-%d')  # date_data_scanned

            property_add = response.css('#property-description .address ::text').getall()  # full_adddress
            property_address = ' '.join(element.strip().replace('\t', ' ') for element in property_add)  # description
            item['Field5'] = property_address.strip()  # full_adddress

            item['Field6'] = response.xpath("//small[contains(text(),'Bed')]/following-sibling::span[1]/text()").get('').strip()
            item['Field7'] = response.xpath("//small[contains(text(),'Bath')]/following-sibling::span[1]/text()").get('').strip()
            item['Field8'] = response.xpath("//small[contains(text(),'Car')]/following-sibling::span[1]/text()").get('').strip()

            # sold_price = response.xpath("//*[contains(@data-testid,'listing-price')]/text()").get('').strip()
            # if sold_price:
            #     item['Field9'] = (sold_price.strip()).replace('$','')   # sold_price

            description = response.css('.title ::text, #property-description p ::text').getall()  # description
            item['Field12'] = ' '.join(element.strip().replace('\t', ' ') for element in description)  # description

            '''         Uploading Images on Wasabi S3 Bucket            '''
            Agent_Imagess = response.css('p.gallery-popup a ::attr(href)').getall()
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

            agent1 = response.css('div.agent')
            agent1_name = agent1.css('.name strong ::text').get('').strip()  # description
            print('Agent Name is :', agent1_name)
            if agent1:
                first_name1, last_name1 = agent1_name.split(maxsplit=1)
                item['Field15'] = first_name1.strip()  # agent_first_name_1
                item['Field16'] = last_name1.strip()  # agent_surname_name_1
                item['Field17'] = agent1_name.strip()  # agent_full_name_1
                agent1_phone = agent1.css("p.contact ::text").get('').strip()
                item['Field20'] = agent1_phone.strip()
                if len(agent1.css("p.contact ::text").getall()) > 1:
                    agent1_mobile = agent1.css("p.contact ::text").getall()
                    item['Field21'] = agent1_mobile[1].strip()
                # item['Field21'] = agent1.xpath("//i[contains(@class,'fa fa-mobile')]/parent::p[1]/text").get('').strip()
                '''         Uploading Images on Wasabi S3 Bucket            '''
                Agent_Imagess = agent1.css(".centerimage ::attr(style)").getall()
                pattern = r"url\((.*?)\)"
                matches = re.search(pattern, Agent_Imagess[0])
                Agent_Images = None
                if matches:
                    Agent_Images = matches.group(1)
                new_name = agent1_name.replace(' ', '')
                id = agent1_phone.strip().replace(' ', '')
                # item['Field22'] = Agent_Images   # listing_images
                # print('Agent1 :',Agent_Images,',', new_name,',', id)
                images = self.uploaded(Agent_Images, new_name, id)  ## ','saperated Images(string), agent_name, agent_id
                item['Field22'] = ", ".join(images)

            if len(response.css('div.agent').getall()) > 1:
                agent2 = response.css('div.agent')[1]
                agent2_name = agent2.css('.name strong ::text').get('').strip()
                if agent2:
                    item['Field23'] = agent2_name.strip()
                    agent2_phone = agent2.css("p.contact ::text").get('').strip()
                    item['Field26'] = agent2_phone.strip()
                    if len(agent2.css("p.contact ::text").getall()) > 1:
                        agent2_mobile = agent2.css("p.contact ::text").getall()
                        item['Field26A'] = agent2_mobile[1].strip()
                    '''         Uploading Images on Wasabi S3 Bucket            '''
                    Agent_Imagess = agent2.css(".centerimage ::attr(style)").getall()
                    pattern = r"url\((.*?)\)"
                    matches = re.search(pattern, Agent_Imagess[0])
                    Agent_Images = None
                    if matches:
                        Agent_Images = matches.group(1)
                    new_name = agent2_name.replace(' ', '')
                    id = agent2_phone.strip().replace(' ', '')
                    # item['Field27'] = Agent_Images
                    # print('Agent2 :',Agent_Images,',', new_name,',', id)
                    images = self.uploaded(Agent_Images, new_name, id)  ## ','saperated Images(string), agent_name, agent_id
                    item['Field27'] = ", ".join(images)

            # item['Field33'] = response.xpath("//b[contains(text(),'Property ID')]/following-sibling::span[1]/text()").get('').strip()
            item['Field35'] = response.css('.property_type .value ::text').get('').strip()

            land_area = response.xpath("//span[contains(text(),'Area')]/following-sibling::span[1]/text()").get('').strip()
            if land_area:
                item['Field36'] = land_area.replace('m2','').replace(' ','')   # land_area

            # feature_count = 58
            # features = response.xpath("//i[contains(@class,'fa fa-check')]/parent::span[1]/preceding-sibling::b[1]")
            # for feature in features:
            #     if feature.css('::text').get('').strip() == 'Air Conditioning':
            #         item['Field52'] = features.css('::text').get('').strip()
            #     else:
            #         item[f'Field{feature_count}'] = feature.css('::text').get('').strip()
            #         feature_count += 1

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

        wasabi_access_key = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
        wasabi_secret_key = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx'
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
