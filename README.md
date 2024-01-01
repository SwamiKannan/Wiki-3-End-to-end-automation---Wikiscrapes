# Scrape pages pertaining to all subcategories of the category you specify
<p align = "center">
<img src="https://github.com/SwamiKannan/WikiCategoryScrapes/blob/main/images/cover.png"
</p>

**Provide the python file with any Category page from Wikipedia (The starting part of the URL is: "https://en.wikipedia.org/wiki/Category:<category name>" e.g. https://en.wikipedia.org/wiki/Category:Physics) and the file will provide you with the complete list of pages of the category as well as as all subcategories under the category provided** 

## Introduction:
My previous repositories were split into two different objectives:
1. [Repo 1](https://github.com/SwamiKannan/Scraping-the-Wikipedia-Category-Hierarchy) - Crawl the category page and download the list of subcategories of each page - basically, map the hierarchy from the parent category
2. [Repo 2](https://github.com/SwamiKannan/Extracting-content-from-Wikidumps-XML-files)- Extract the XML file from Wiki:Export and convert it to a JSON file

There are two issues using the two repos together:
1. There is still a manual step where you have to copy paste the page names into the Wiki [Special:Export](https://en.wikipedia.org/wiki/Special:Export) page
2. For a large number of pages, there are issues with generating the XML file. The complete XML file was not getting generated after around 20K page names and hence, would lead to errors during the XML parsing phase

## Structure:
The code works as follows:
<p align='center'>
<img src="https://github.com/SwamiKannan/End-to-end-automation---Wikiscrapes/blob/main/images/final_flow.png"><br>
  <b> Legend: </b> <br>
  <img src="https://github.com/SwamiKannan/End-to-end-automation---Wikiscrapes/blob/main/images/legend.png">
</p>

## Usage
### 1. Download the git file
```
git clone https://github.com/SwamiKannan/Wiki-3-End-to-end-automation---Wikiscrapes.git
```
### 2. Pip install the requirements
Through the command window, navigate to the git folder and run:
```
pip install -r requirements.txt
```
#### Note 1: This assumes that you have already python, and the pip and git libraries installed.

### 3. Decide your parameters
1. Get the URL from where you want to scrape the subcategories and pages. This URL must be a **category** page in Wikipedia i.e. URL of the format: **https://en.wikipedia.org/wiki/Category:**
2. Decide on the maximum number of sub-categories you would like to scrape (optional)
3. Decide on the maximum number of page names you would like to extract (optional)
4. Decide on the depth of the category tree that you would like to extract the page names for (depth is explained in the cover image above)
   
#### Note 2: If you provide (2), (3) and (4), which ever criteria is met first will halt the scraping
#### Note 3: If you do not provide (2) or (3) or (4) above, the script will keep running until all subcategories are exhausted. This is not recommended since within 7 levels of depth, you can go from Physics to Selena Gomez' We Own the Night Tour page as below:
   <p align = "center">
   <img src="https://github.com/SwamiKannan/Scraping_Wikipedia_categories/blob/main/images/depth_gone_wrong.png">
   </p>
 
