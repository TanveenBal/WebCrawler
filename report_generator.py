from scraper import ScraperData
import heapq

def report_generator():
    # Retrieve information about the longest page
    longest_page_url, longest_page_words = ScraperData.get_longest_info()
    # Sort subdomains alphabetically
    sorted_subdoms = sorted(ScraperData.subdomains.items(), key=lambda x: (x[0], x[1]))
    # Get the top 50 tokens
    top_fifty = heapq.nlargest(50, ScraperData.all_tokens.items(), key=lambda item: item[1])

    with open('report.txt', 'w') as f:
        f.write(f"Number of Unique Links: {len(ScraperData.get_unique_links_keys())}\n") 
        f.write("\n")

        f.write(f"Longest Page: {longest_page_url}, {longest_page_words}\n")
        f.write("\n")

        f.write(f"ics.uci.edu Subdomains\n")
        for i in sorted_subdoms:
            f.write(f"{i[0]}, {i[1]}\n")
        f.write("\n")

        f.write(f"Top 50 Words\n")
        for ind, item in enumerate(top_fifty):
            f.write(f"{ind+1}. {item[0]} : {item[1]}\n")
    
    return True
