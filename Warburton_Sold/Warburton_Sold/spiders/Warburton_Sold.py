########    Warburton_Sold
import re

import scrapy
import pymongo
import mimetypes
from datetime import datetime
import requests, json, time, threading, queue, os
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
class Warburton_Sold(scrapy.Spider):
    name = 'Warburton_Sold'
    prefix = 'https://warburtonre.com.au'
    url = "https://warburtonre.com.au/sold-properties"
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'en-US,en;q=0.9,ur;q=0.8,nl;q=0.7',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        # 'Cookie': 'AMP_MKTG_16a5c84b5b=JTdCJTdE; VID=1260127; ProtoCID=vuHl0xXHIGeC61DEFkdc70; _gid=GA1.3.1161930921.1713434394; _ga_PGWTZ3K8W8=GS1.1.1713434402.5.1.1713434419.0.0.0; _ga=GA1.1.768639525.1712938573; AMP_16a5c84b5b=JTdCJTIyZGV2aWNlSWQlMjIlM0ElMjIxYjY2OGU3NS0xYmY4LTQwNDgtOTRkNi02NTJhNzMyMTRjNTIlMjIlMkMlMjJzZXNzaW9uSWQlMjIlM0ExNzEzNDM0Mzg3MDc2JTJDJTIyb3B0T3V0JTIyJTNBZmFsc2UlMkMlMjJsYXN0RXZlbnRUaW1lJTIyJTNBMTcxMzQzNDQxOTc5OCU3RA==; _ga_89Q3N1K75B=GS1.3.1713434403.4.1.1713434425.0.0.0; arp_scroll_position=2460',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"'
    }

    count = 1
    db = 'WarburtonRE'
    collection = 'Warburton_Sold'
    bucket_prefix = f'D_{collection}'

    def start_requests(self):
        yield scrapy.Request(url=self.url, callback=self.parse, headers=self.headers)

    def parse(self, response):
        for property in response.css(".propertyTile a"):
            property_url = property.css('::attr(href)').get('').strip()
            print(self.count, property_url)
            self.count += 1
            yield scrapy.Request(url=property_url, headers=self.headers, callback=self.Detail_parse)

        if 'page' in response.css(".next ::attr(href)").get('').strip():
            next_page = response.css('.next ::attr(href)').get('').strip()
            yield response.follow(url=next_page, callback=self.parse, headers=self.headers)

    def Detail_parse(self, response):
        DB_already_exists = self.read_data_base(response.url)
        if not DB_already_exists:
            item = dict()
            item['Field2'] = '1177'
            item['Field3'] = datetime.now().date().strftime('%Y-%m-%d')  # date_data_scanned
            item['Field4'] = 'Warburton Real Estate'

            property_add = response.css('h1 span ::text').getall()  # full_adddress
            property_address = ' '.join(element.strip().replace('\n', ' ').replace('\t','') for element in property_add)  # description
            item['Field5'] = property_address.strip()  # full_adddress

            bed_bath_car = response.css('div.icons ::text').getall()
            if response.css('.icon-bed').get() in response.css('.icons').get():
                item['Field6'] = bed_bath_car[0].strip()
            if response.css('.icon-bath').get() in response.css('.icons').get():
                item['Field7'] = bed_bath_car[1].strip()
            if response.css('.icon-car').get() in response.css('.icons').get():
                item['Field8'] = bed_bath_car[2].strip()

            sold_price = response.css(".muted ::text").get('').strip().replace(' ','')
            if sold_price:
                item['Field9'] = (sold_price.strip()).replace('$','')   # sold_price

            description = response.css('h1+ .contentRegion p ::text').getall()  # description
            item['Field12'] = ' '.join(element.strip().replace('\t', ' ').replace('Read More','') for element in description)  # description

            '''         Uploading Images on Wasabi S3 Bucket            '''
            Agent_Imagess = response.css(".widescreen link ::attr(href)").getall()
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
            agent1 = response.css(".grid-med-1 li")[0]
            agent1_name = agent1.css('.oneline ::text').get('').strip()
            if agent1:
                first_name1, last_name1 = agent1_name.split(maxsplit=1)
                item['Field15'] = first_name1.strip()  # agent_first_name_1
                item['Field16'] = last_name1.strip()  # agent_surname_name_1
                item['Field17'] = agent1_name.strip()  # agent_full_name_1
                '''         Uploading Images on Wasabi S3 Bucket            '''
                Agent_Images_style = agent1.css(".portrait ::attr(style)").get('').strip()
                Agent_Images = self.prefix + re.search(r"url\((.*?)\)", Agent_Images_style).group(1) if "url" in Agent_Images_style else ''
                new_name = agent1_name.replace(' ', '')
                id = (response.url).split('/')[-2].replace(' ', '')
                # item['Field22'] = Agent_Images   # listing_images
                # print('Agent1 :',Agent_Images,',', new_name,',', id)
                images = self.uploaded(Agent_Images, new_name, id)  ## ','saperated Images(string), agent_name, agent_id
                item['Field22'] = ", ".join(images)

            if len(response.css(".grid-med-1 li").getall()) > 1:
                agent2 = response.css(".grid-med-1 li")[1]
                agent2_name = agent2.css('.oneline ::text').get('').strip()
                if agent2:
                    item['Field23'] = agent2_name.strip()
                    '''         Uploading Images on Wasabi S3 Bucket            '''
                    Agent_Images_style = agent2.css(".portrait ::attr(style)").get('').strip()
                    Agent_Images = self.prefix + re.search(r"url\((.*?)\)", Agent_Images_style).group(1) if "url" in Agent_Images_style else ''
                    new_name = agent2_name.replace(' ', '')
                    id = (response.url).split('/')[-2].replace(' ', '')
                    # item['Field27'] = Agent_Images
                    # print('Agent2 :',Agent_Images,',', new_name,',', id)
                    images = self.uploaded(Agent_Images, new_name, id)  ## ','saperated Images(string), agent_name, agent_id
                    item['Field27'] = ", ".join(images)

            if len(response.css(".grid-med-1 li").getall()) == 3:
                agent3 = response.css(".grid-med-1 li")[2]
                agent3_name = agent3.css('.oneline ::text').get('').strip()
                if agent3:
                    item['Field28'] = agent3_name.strip()
                    '''         Uploading Images on Wasabi S3 Bucket            '''
                    Agent_Images_style = agent3.css(".portrait ::attr(style)").get('').strip()
                    Agent_Images = self.prefix + re.search(r"url\((.*?)\)", Agent_Images_style).group(1) if "url" in Agent_Images_style else ''
                    new_name = agent3_name.replace(' ', '')
                    id = (response.url).split('/')[-2].replace(' ', '')
                    # item['Field32'] = Agent_Images
                    # print('Agent3 :',Agent_Images,',', new_name,',', id)
                    images = self.uploaded(Agent_Images, new_name, id)  ## ','saperated Images(string), agent_name, agent_id
                    item['Field32'] = ", ".join(images)

            # item['Field33'] = response.xpath("//label[contains(text(),'Property ID')]/following-sibling::div[1]/text()").get('').strip()
            # item['Field35'] = bed_bath_car[-1].strip()

            land_area = response.xpath("//*[contains(text(),'Land area')]/following-sibling::span[1]/text()").get('').strip()
            if 'ha' in land_area:
                ha = float(land_area.replace('ha','').replace(' ',''))
                item['Field36'] = ha * 10000
            elif 'ac' in land_area:
                acres = float(land_area.replace('ac', '').replace(' ',''))
                item['Field36'] = acres * 4046.86
            elif 'sqm' in land_area:
                item['Field36'] = land_area.replace('sqm','').replace('Land size:','').replace(' ','')   # land_area

            feature_count = 58
            for feature in response.xpath("//*[contains(text(),'Property Features')]/following-sibling::ul[1]/li"):
                featureee = ' '.join(element.strip() for element in feature.css('::text').getall())
                if 'Air Conditioning' in featureee:
                    item['Field52'] = featureee.strip()
                else:
                    item[f'Field{feature_count}'] = featureee.strip()
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
