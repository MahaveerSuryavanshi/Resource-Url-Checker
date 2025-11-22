import streamlit as st
import pandas as pd
import aiohttp
import asyncio
from io import BytesIO

# ---------------------------------------
# ASYNC URL CHECK FUNCTION
# ---------------------------------------
async def fetch_status(session, url, timeout=8):
    try:
        async with session.head(url, timeout=timeout, allow_redirects=True) as response:
            return url, response.status < 400
    except:
        return url, False


async def check_urls_async(url_list):
    results = []
    timeout = aiohttp.ClientTimeout(total=10)

    connector = aiohttp.TCPConnector(limit=200)  # high concurrency
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        tasks = [fetch_status(session, url) for url in url_list]

        completed = 0
        progress = st.progress(0)

        for coro in asyncio.as_completed(tasks):
            result = await coro
            results.append(result)

            completed += 1
            progress.progress(completed / len(url_list))

    return results


# ---------------------------------------
# STREAMLIT UI
# ---------------------------------------
st.title("Resource URL Checker - BHU Libray ")
st.write("Upload a CSV or Excel file with a column named **url**.")


uploaded_file = st.file_uploader("Upload file", type=["csv", "xlsx"])

if uploaded_file:
    # Load Data
    if uploaded_file.name.endswith(".csv"):
        df = pd.read_csv(uploaded_file)
    else:
        df = pd.read_excel(uploaded_file)

    if "url" not in df.columns:
        st.error("The file must contain a column named 'url'")
        st.stop()

    st.write("### Preview of Uploaded Data:")
    st.dataframe(df.head())

    if st.button("Start URL Check"):
        urls = df["url"].dropna().tolist()

        st.write(f"### Checking {len(urls)} URLs... This may take a moment.")

        # Run async checking
        results = asyncio.run(check_urls_async(urls))

        # Split results
        working = [u for u, status in results if status]
        nonworking = [u for u, status in results if not status]

        df_working = pd.DataFrame({"url": working})
        df_nonworking = pd.DataFrame({"url": nonworking})

        st.success("URL Checking Complete! 🎉")

        st.write("### ✔ Working URLs")
        st.dataframe(df_working)

        st.write("### ❌ Non-working URLs")
        st.dataframe(df_nonworking)

        # Download buttons
        def to_csv_bytes(df):
            return df.to_csv(index=False).encode("utf-8")

        st.download_button(
            label="⬇ Download Working URLs",
            data=to_csv_bytes(df_working),
            file_name="working_urls.csv",
            mime="text/csv",
        )

        st.download_button(
            label="⬇ Download Non-working URLs",
            data=to_csv_bytes(df_nonworking),
            file_name="nonworking_urls.csv",
            mime="text/csv",
        )
