from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.options import Options 
import pandas as pd
import bs4
import requests
import re
import numpy as np


# Get Chrome Driver path and set the crawler to run in headless option
chrome_options = Options()  
chrome_options.add_argument("--headless") 
chrome_path=r"C:\Users\rongshiu\Desktop\chromedriver"
driver=webdriver.Chrome(chrome_path,options=chrome_options)


# Prompt user for url and number of pages within the category for the crawler to loop through
link=str(input('Please Enter category URLto crawl: ').strip())
pages=int(input('Please Enter number of pages to iterate over: ').strip())+1


# Use selenium to perform clicking process for each product and bs4 to perform the scrapping after entering the targetted page
records=[]
for i in range(1,pages):
    driver.get('{}}/{}'.format(link,i))
    details_containers=driver.find_elements_by_css_selector('div.name>a')
    for details_container in details_containers:
        ActionChains(driver).key_down(Keys.CONTROL).click(details_container).key_up(Keys.CONTROL).perform()
        current_window = driver.current_window_handle
        new_window = [window for window in driver.window_handles if window != current_window][0]
        driver.switch_to.window(new_window)

        html=driver.page_source
        soup=bs4.BeautifulSoup(html,'lxml')
            
        if soup.find('div',class_='flag') != None:
            product_name=soup.find('li',class_='breadcrumb-item active').text.strip()
            stock_code_container=soup.find('div',class_='model')
            stock_code=stock_code_container.find('div',class_='value').text.strip()
            price_container=soup.find('div',class_='price')
            if price_container.find('div',class_='value')!=None:
                price=price_container.find('div',class_='value').text.strip()
            else:
                price=""
            stock_container=soup.find('div',class_='stock')
            stock=stock_container.find('div',attrs={'class':re.compile('value ')}).text.strip()
            if soup.find('div',class_='photo')!=None:
                image_url=soup.find('div',class_='photo')
                image_srcs = [img['src'] for img in image_url.select('img[src]')][0]
            else:
                image_srcs =""
            page_num=i
            if soup.find('div',class_='description')!= None and soup.find('div',class_='details')!= None:
                des=soup.find('div',class_='description')
                detail=soup.find('div',class_='details')
                description=str(des)+" "+str(detail)
            elif soup.find('div',class_='description')!= None:
                description=soup.find('div',class_='description')
            elif soup.find('div',class_='details')!= None:
                description=soup.find('div',class_='details')
            else:
                description=""
              
            records.append((product_name,stock_code,price,stock,description,image_srcs,page_num))
        
            driver.close()
            driver.switch_to.window(current_window)
        else:
            driver.close()
            driver.switch_to.window(current_window)


#Store crawled data into a dataframe
df1=pd.DataFrame(records,columns=['Product Name','Stock Code','Price','Stock','Description','Base Image','Page #'])
df1['Product Name'].replace('', np.nan, inplace=True)
df1['Stock'].replace('0', np.nan, inplace=True)
df1.dropna(subset=['Product Name','Stock'], inplace=True)
print(df1.head())


# Perform ETL to fit data as per existing DB table format
df2=pd.DataFrame(columns=['Seller_id','model','sku','description:name:English','description:meta_title:English','description:description:English','quantity','stock_status','stock_status_name','image','shipping','price','tax_class','weight','weight_class','weight_class_name','category'])
['description:name:English']=df1['Product Name']
df2['description:description:English']=df1['Description']
df2['model']=df1['Stock Code']
df2['description:meta_title:English']=df2['description:name:English']
df2['sku']=df2['model']
df2['stock_status']=7
df2['stock_status_name']='In Stock'
df2['quantity']=df1['Stock']
df2['shipping']=1
df2['tax_class']=0
df2['weight']=0.5
df2['weight_class']=1
df2['weight_class_name']='Kilogram'
df2['category']="Games, Books & Hobbies/Books/Religious Book (Christianity)"
df2['Seller_id']=72
df2['price']=df1['Price']


# Perform ETL to store Image to a default vendor wkseller/72 prior uploading via FTP
df2['image']=df1['Base Image'].fillna('')
def image_format(x):
    list1=re.split(r'/',x)
    
    if len(list1)>0:
        image_link='wkseller/72/'+ list1[-1]
    else:
        image_link=''
    
    return image_link

df2['image']=df2['image'].apply(image_format)
df2.to_csv('magazines.csv',encoding='utf-8-sig',index=False)
driver.quit()

url_list=df1['Base Image'].tolist()
for url in url_list:
    filename = url.split('/')[-1]
    filepath='C:/Users/edward.chong/Desktop/ScrapImg/Chinese'
    r = requests.get(url, allow_redirects=True)
    open(filepath+'/'+filename, 'wb').write(r.content)

