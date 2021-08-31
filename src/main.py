from src.statistics import PageUserCount

def main():
    PageUserCount(schema="pageId string, userId string, timestamp long").run

if __name__ =="__main__":
    main()