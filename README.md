# 💣 Counter-Strike: Tactical Data Analysis

### Where Data Science Meets the Server

![CS2 Data Banner](https://img.shields.io/badge/CS2-Data%20Analysis-orange?style=for-the-badge&logo=counter-strike) ![Python](https://img.shields.io/badge/Made%20with-Python-blue?style=for-the-badge&logo=python) ![Status](https://img.shields.io/badge/Status-Active-brightgreen?style=for-the-badge)

## 📖 About This Branch

Welcome to my **Counter-Strike Analysis** repository. 

I have been passionate about Counter-Strike for years, fascinated not just by the mechanical skill required to compete at the highest level, but by the complex strategic layers that drive the professional scene. 

As a Data Scientist, I view every match as a dataset waiting to be solved. This branch is dedicated to parsing professional demo files, analyzing team economies, and visualizing player behavior to uncover the hidden statistics that define the "meta."

## 🎯 Project Goals

The objective of this analysis is to go beyond the K/D ratio and explore the deeper metrics that lead to round wins.

- **Economy Efficiency:** Analyzing how teams manage their money and the ROI of force-buy rounds.
- **Utility Usage:** Heatmapping smoke and flashbang coordinates to determine effective map control.
- **Entry Pathing:** Using clustering algorithms to determine the most successful opening routes on competitive maps.
- **Clutch Probability:** Leveraging machine learning (Random Forest/Logistic Regression) to predict round outcomes based on live gamestate variables.

## 🛠️ Tech Stack & Tools

This branch and all projects on it utilizes a Python-based stack for demo parsing and visualization:

* **Core:** Python 3.9+
* **Data Manipulation:** Pandas, NumPy
* **Visualization:** Matplotlib, Seaborn
* **Demo Parsing:** `awpy` (or `demoparser2`)
* **Machine Learning:** Scikit-Learn
