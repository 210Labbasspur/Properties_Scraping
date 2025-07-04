########### MD_Sale
import scrapy
import pymongo
import mimetypes
from datetime import datetime
import requests, json, time, threading, queue, os
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
class MD_Sale(scrapy.Spider):
    name = 'MD_Sale'
    prefix = 'https://multidynamic.com.au'
    url = ("https://multidynamic.com.au/search-properties?category=buy&sub_category=1&type=any&location=any&bedrooms=any&bathrooms"
           "=any&garages=any&price_range=0%2C600000&page={}")
    headers = {
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.9,ur;q=0.8,nl;q=0.7',
        'Connection': 'keep-alive',
        # 'Cookie': '_gid=GA1.3.2126078576.1712006508; XSRF-TOKEN=eyJpdiI6IlFqWE1VVVpmaElhM0hCK01oK0t1emc9PSIsInZhbHVlIjoiQWhiSkdSZVJOZkJmRnFHSEtFRExRZDIwS1FJNVR4c2lSWWxOZ1wvbFN4MHVKRkFScmRwY0FUb3FGZFU0Q2xkbVIiLCJtYWMiOiI5YzNlMjIyM2VlMjM2NGJiMmJjZWIxYzI0ZDcxNzBmNDczNjVhMzJmZTUxMGYzZTBlYjA4YmM1NjI2ZjA3ZDI4In0%3D; multi_dynamic_session=eyJpdiI6IjRNek9zZ2o3eHdtanpEeTVMZFwvS0ZBPT0iLCJ2YWx1ZSI6IlZCWVFzSkxxOFBhQ2NvdTlQQUJNczVDTkg0M3hqbmQ1ZnpXMFlCMzF4azB0OTZLTmFxQlQzZzBKSFFkeFZNZ0QiLCJtYWMiOiI1ZmJjOGQ0MTNmYjhjOTM1NzRiMTNiMzNmNTE2MzE0Y2RhZDVmN2NjODIxMmE2NjAyZTU5MGIzYjE1MjYzNGQzIn0%3D; _ga=GA1.1.560681732.1711470312; _ga_VKYH3K9SSQ=GS1.1.1712006507.2.1.1712007841.25.0.0; _ga_CSBD8TEWL5=GS1.1.1712006507.2.1.1712007841.0.0.0; arp_scroll_position=1181; XSRF-TOKEN=eyJpdiI6IlRwd05TdEpKU1FqZEZQVTNUaFRLcWc9PSIsInZhbHVlIjoiMlRvaHBBNlJSdUhjRTVIZGRMaGZLTmdiNHhaSENuK0tTXC9Fd0J2VE5zWmJtRndEQnBoMlBIdTZJUVFjeW5iVjUiLCJtYWMiOiI5MmUxNTljNWIxMmNiMzdiZmY4N2NhZTYzMDliMGVhMjIzODE3ZjMwZTg1M2NiMmRkODU5NDg1N2EzNWJkZDBlIn0%3D; multi_dynamic_session=eyJpdiI6IjNreCtlWlhUYWlWU3g2ZVhjcHk5elE9PSIsInZhbHVlIjoiVWl5OUhyc2lzNFJzazdkWkk0MlY3Unl5WDY5Z0FzQVdyYmpabEdQdjh0RjNNOU9yYndiVEoxbkY4VXV4akVrZiIsIm1hYyI6IjE4MzE4Yzk5NTA5ZWNkMzFkZDRjMTc4MTU3NWQwMjY3Y2Y3NzRiMjllOTE0MGIxODQxZWEzOWY4MTE2Y2YxMGYifQ%3D%3D',
        # 'Referer': 'https://multidynamic.com.au/properties?category=buy&subcategory=residential',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
        'X-CSRF-TOKEN': 'SB6uZQGkDJMBf5fer8cVNQl34xVKeEl0kKSsmHVs',
        'X-Requested-With': 'XMLHttpRequest',
        'sec-ch-ua': '"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"'
    }

    count = 1
    db = 'MultiDynamic'
    collection = 'MultiDynamic_Sale'
    bucket_prefix = f'D_{collection}'

    local_file_path = '/img'

    def start_requests(self):
        page_no = 1
        yield scrapy.Request(url=self.url.format(page_no), headers=self.headers, callback=self.parse, meta={'page_no':page_no})

    def parse(self, response):
        data = json.loads(response.text)
        for property in data['properties']:
            property_url = 'https://multidynamic.com.au/show-property-details/' + property.get('slug')
            print(self.count, property_url)
            self.count += 1
            yield scrapy.Request(url=property_url, headers=self.headers, callback=self.Detail_parse)

        '''    PAGINATION    '''
        if 'rel=\"next\"' in data['view']:
            page_no = response.meta.get('page_no') + 1
            yield scrapy.Request(url=self.url.format(page_no), headers=self.headers, callback=self.parse,
                                 meta={'page_no': page_no})

    def Detail_parse(self, response):
        DB_already_exists = self.read_data_base(response.url)
        if not DB_already_exists:
            item = dict()
            item['Field2'] = '1128'
            item['Field3'] = datetime.now().date().strftime('%Y-%m-%d')  # date_data_scanned

            property_add = response.css('.single-title-block h1 ::text').getall()  # full_adddress
            property_address = ' '.join(element.strip().replace('\n', ' ') for element in property_add)  # description
            item['Field5'] = property_address.strip()  # full_adddress

            item['Field6'] = response.css('.item-bed ::text').get('').strip().replace(' Bed','').replace('\n', '').replace(' ','')
            item['Field7'] = response.css('.item-bath ::text').get('').strip().replace(' Bath','').replace('\n', '').replace(' ','')
            item['Field8'] = response.css('.item-garage ::text').get('').strip().replace(' Garage','').replace('\n', '').replace(' ','')

            # sold_price = response.xpath("//*[contains(@data-testid,'listing-price')]/text()").get('').strip()
            # if sold_price:
            #     item['Field9'] = (sold_price.strip()).replace('$','')   # sold_price

            description = response.css('.description ::text').getall()  # description
            item['Field12'] = ' '.join(element.strip().replace('\t', ' ').replace('\n','') for element in description)  # description
            item['Field12'] = item['Field12'].replace('Read more','')

            '''         Uploading Images on Wasabi S3 Bucket            '''
            Agent_Imagess = response.css('.img-fluid ::attr(src)').getall()
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

            # agent1 = response.css('.teamList-block-content .agentTile')
            # agent1_name = agent1.css('.agentTile-name ::text').get('').strip()  # description
            # if agent1:
            #     first_name1, last_name1 = agent1_name.split(maxsplit=1)
            #     item['Field15'] = first_name1.strip()  # agent_first_name_1
            #     item['Field16'] = last_name1.strip()  # agent_surname_name_1
            #     item['Field17'] = agent1_name.strip()  # agent_full_name_1
            #     item['Field18'] = agent1.css('.agentTile-title ::text').get('').strip()  # agent_title_1
            #     agent1_phone = agent1.css(".agentTile-contact-mobile a ::text").get('').strip()
            #     item['Field20'] = agent1_phone.strip()
            #     '''         Uploading Images on Wasabi S3 Bucket            '''
            #     Agent_Imagess = agent1.css(".agentTile-image ::attr(data-bg-src)").getall()
            #     Agent_Imagesss = []
            #     for url in Agent_Imagess:
            #         Agent_Imagesss.append(self.prefix + url)
            #     Agent_Images = ','.join(Agent_Imagesss[:1])
            #     new_name = agent1_name.replace(' ', '')
            #     id = agent1_phone.strip().replace(' ', '')
            #     # item['Field22'] = Agent_Images   # listing_images
            #     # print('Agent1 :',Agent_Images,',', new_name,',', id)
            #     images = self.uploaded(Agent_Images, new_name, id)  ## ','saperated Images(string), agent_name, agent_id
            #     item['Field22'] = ", ".join(images)
            #
            # if len(response.css('.teamList-block-content .agentTile').getall()) > 1:
            #     agent2 = response.css('.teamList-block-content .agentTile')[1]
            #     agent2_name = agent2.css('.agentTile-name ::text').get('').strip()  # description
            #     if agent2:
            #         item['Field23'] = agent2_name.strip()
            #         item['Field24'] = agent2.css('.agentTile-title ::text').get('').strip()  # agent_title_1
            #         agent2_phone = agent2.css(".agentTile-contact-mobile a ::text").get('').strip()
            #         item['Field26'] = agent2_phone.strip()
            #         '''         Uploading Images on Wasabi S3 Bucket            '''
            #         Agent_Imagess = agent2.css(".agentTile-image ::attr(data-bg-src)").getall()
            #         Agent_Imagesss = []
            #         for url in Agent_Imagess:
            #             Agent_Imagesss.append(self.prefix + url)
            #         Agent_Images = ','.join(Agent_Imagesss[:1])
            #         new_name = agent2_name.replace(' ', '')
            #         id = agent2_phone.strip().replace(' ', '')
            #         # item['Field27'] = Agent_Images
            #         # print('Agent2 :',Agent_Images,',', new_name,',', id)
            #         images = self.uploaded(Agent_Images, new_name, id)  ## ','saperated Images(string), agent_name, agent_id
            #         item['Field27'] = ", ".join(images)

            # # item['Field33'] = response.xpath("//b[contains(text(),'Property ID')]/following-sibling::span[1]/text()").get('').strip()
            # item['Field35'] = response.xpath("//span[contains(text(),'Property Type')]/following-sibling::span[1]/text()").get('').strip()
            #
            # land_area = response.xpath("//span[contains(text(),'Land area ')]/following-sibling::span[1]/text").get('').strip()
            # if land_area:
            #     item['Field36'] = land_area.replace('sqm','').replace(' ','')   # land_area
            #
            # feature_count = 58
            # features = response.css('.highlightbox~ .highlightbox li')
            # for feature in features:
            #     item[f'Field{feature_count}'] = ' '.join(element.strip() for element in feature.css('::text').getall())
            #     feature_count += 1
            item['Field35'] = response.css('.badges span ::text').get('').strip()

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
