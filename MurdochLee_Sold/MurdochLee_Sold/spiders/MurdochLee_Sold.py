########        MurdochLee_Sold
import scrapy
import pymongo
import mimetypes
from datetime import datetime
import requests, json, time, threading, queue, os
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
class MurdochLee_Sold(scrapy.Spider):
    name = 'MurdochLee_Sold'
    prefix = 'https://murdochlee.com.au'
    url = "https://murdochlee.com.au/sell/recent-sales/"
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9,ur;q=0.8,nl;q=0.7',
        'cache-control': 'max-age=0',
        # 'cookie': 'epl_wp_session=e7b44cf5c5b0261ebca826eaf4520df9%7C%7C1713127158%7C%7C1713125358; preferredView=grid; arp_scroll_position=836',
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
    db = 'MurdochLee'
    collection = 'MurdochLee_Sold'
    bucket_prefix = f'D_{collection}'

    def start_requests(self):
        yield scrapy.Request(url=self.url, callback=self.parse, headers=self.headers)

    def parse(self, response):
        for property in response.css(".epl-rec-address a"):
            property_url = property.css('::attr(href)').get('').strip()
            print(self.count, property_url)
            self.count += 1
            yield scrapy.Request(url=property_url, headers=self.headers, callback=self.Detail_parse)

        if 'page' in response.css(".nextpostslink ::attr(href)").get('').strip():
            next_page = response.css('.nextpostslink ::attr(href)').get('').strip()
            yield scrapy.Request(url=next_page, callback=self.parse, headers=self.headers)

    def Detail_parse(self, response):
        DB_already_exists = self.read_data_base(response.url)
        if not DB_already_exists:
            item = dict()
            item['Field2'] = '1080'
            item['Field3'] = datetime.now().date().strftime('%Y-%m-%d')  # date_data_scanned
            item['Field4'] = 'MurdochLee'

            property_add = response.css('.epl-row-left ::text').getall()  # full_adddress
            property_address = ' '.join(element.strip().replace('\n', ' ').replace('\t','') for element in property_add)  # description
            item['Field5'] = property_address.strip()  # full_adddress

            item['Field6'] = response.css(".bedroom .rec-icon-value ::text").get('').strip()
            item['Field7'] = response.css(".bathroom .rec-icon-value ::text").get('').strip()
            item['Field8'] = response.css(".parking .rec-icon-value ::text").get('').strip()

            sold_price = response.css(".sold-status ::text").get('').strip().replace('Sold','').replace(' ','')
            if sold_price:
                item['Field9'] = (sold_price.strip()).replace('$','')   # sold_price

            description = response.css('.epl-block-content-section ::text').getall()  # description
            item['Field12'] = ' '.join(element.strip().replace('\t', ' ') for element in description)  # description

            '''         Uploading Images on Wasabi S3 Bucket            '''
            # Agent_Imagess = response.css(".slick-slide img ::attr(data-lazy)").getall()
            Agent_Imagess = response.xpath("//*[contains(@class,'frame slick-slide epl-slick-item epl-slider-current-slide')]/a/@href").getall()
            new_img_urls = []
            for url in Agent_Imagess:
                new_img_urls.append(url)
            Images = ', '.join(new_img_urls)
            new_name = self.db.replace(' ', '').replace('/', '_').replace('\\','')
            id = (response.url).split('/')[-2].replace(' ', '').replace('/', '_').replace('\\','')
            # item['Field13'] = new_img_urls   # listing_images
            # print(Images,new_name,id)
            images = self.uploaded(Images, new_name, id)  ## ','saperated Images(string), Property_address, URL_id
            item['Field13'] = ", ".join(images)

            item['Field14'] = response.url  # external_link
            '''          AGENTS          '''
            agent1 = response.xpath("//*[contains(@class,'epl-author-box-outer-wrapper epl-clearfix')]")[0]
            agent1_name = agent1.css('.epl-author-title a ::text').get('').strip()
            if agent1_name:
                first_name1, last_name1 = agent1_name.split(maxsplit=1)
                item['Field15'] = first_name1.strip()  # agent_first_name_1
                item['Field16'] = last_name1.strip()  # agent_surname_name_1
                item['Field17'] = agent1_name.strip()  # agent_full_name_1
                item['Field18'] = agent1.css('.epl-sd-entry-position ::text').get('').strip()
                item['Field19'] =  agent1.xpath(".//a[contains(@href,'mailto:')]/@href").get('').strip().replace('mailto:','')
                agent1_phone = agent1.css(".epl-sd-entry-mobile ::text").get('').strip()
                item['Field20'] = agent1_phone.strip()
                '''         Uploading Images on Wasabi S3 Bucket            '''
                Agent_Images = agent1.css(".size-regular ::attr(src)").get('').strip()
                new_name = agent1_name.replace(' ', '')
                id = agent1_phone.strip().replace(' ', '')
                # item['Field22'] = Agent_Images   # listing_images
                # print('Agent1 :',Agent_Images,',', new_name,',', id)
                images = self.uploaded(Agent_Images, new_name, id)  ## ','saperated Images(string), agent_name, agent_id
                item['Field22'] = ", ".join(images)

            if len(response.xpath("//*[contains(@class,'epl-author-box-outer-wrapper epl-clearfix')]").getall()) > 1:
                agent2 = response.xpath("//*[contains(@class,'epl-author-box-outer-wrapper epl-clearfix')]")[1]
                agent2_name = agent2.css('.epl-author-title a ::text').get('').strip()
                if agent2_name:
                    item['Field23'] = agent2_name.strip()
                    item['Field24'] = agent2.css('.epl-sd-entry-position ::text').get('').strip()
                    item['Field25'] = agent2.xpath(".//a[contains(@href,'mailto:')]/@href").get('').strip().replace('mailto:', '')
                    agent2_phone = agent2.css(".epl-sd-entry-mobile ::text").get('').strip()
                    item['Field26'] = agent2_phone.strip()
                    '''         Uploading Images on Wasabi S3 Bucket            '''
                    Agent_Images = agent2.css(".size-regular ::attr(src)").get('').strip()
                    new_name = agent2_name.replace(' ', '')
                    id = agent2_phone.strip().replace(' ', '')
                    # item['Field27'] = Agent_Images
                    # print('Agent2 :',Agent_Images,',', new_name,',', id)
                    images = self.uploaded(Agent_Images, new_name, id)  ## ','saperated Images(string), agent_name, agent_id
                    item['Field27'] = ", ".join(images)

            if len(response.xpath("//*[contains(@class,'epl-author-box-outer-wrapper epl-clearfix')]").getall()) == 3:
                agent3 = response.xpath("//*[contains(@class,'epl-author-box-outer-wrapper epl-clearfix')]")[2]
                agent3_name = agent3.css('.epl-author-title a ::text').get('').strip()
                if agent3_name:
                    item['Field28'] = agent3_name.strip()
                    item['Field29'] = agent3.css('.epl-sd-entry-position ::text').get('').strip()
                    item['Field30'] = agent3.xpath(".//a[contains(@href,'mailto:')]/@href").get('').strip().replace('mailto:', '')
                    agent3_phone = agent3.css(".agent_phone span ::text").get('').strip()
                    item['Field31'] = agent3_phone.strip()
                    '''         Uploading Images on Wasabi S3 Bucket            '''
                    Agent_Images = agent3.css(".size-regular ::attr(src)").get('').strip()
                    new_name = agent3_name.replace(' ', '')
                    id = agent3_phone.strip().replace(' ', '')
                    # item['Field32'] = Agent_Images
                    # print('Agent3 :',Agent_Images,',', new_name,',', id)
                    images = self.uploaded(Agent_Images, new_name, id)  ## ','saperated Images(string), agent_name, agent_id
                    item['Field32'] = ", ".join(images)

            # item['Field33'] = response.xpath("//label[contains(text(),'Property ID')]/following-sibling::div[1]/text()").get('').strip()
            # item['Field35'] = response.xpath("//label[contains(text(),'Type')]/following-sibling::div[1]/text()").get('').strip()

            # land_area = response.css("p.land_size  ::text").get('').strip()
            # if land_area:
            #     item['Field36'] = land_area.replace('m2','').replace('Land size:','').replace(' ','')   # land_area

            # feature_count = 58
            # for feature in response.css(".col-sm-4"):
            #     if 'Air Conditioning' in feature.css('::text').get('').strip():
            #         item['Field52'] = feature.css('::text').get('').strip()
            #     else:
            #         item[f'Field{feature_count}'] = feature.css('::text').get('').strip()
            #         feature_count += 1

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
