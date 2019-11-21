from luigi import build
from .tasks import CleanedReviews
import os

def main():
    build([
        CleanedReviews(

        )], local_scheduler=True)
