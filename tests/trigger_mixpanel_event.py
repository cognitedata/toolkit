import sys


def main():
    sys.argv = ["cdf", "--test=trigger_mixpanel_event"]
    print(sys.argv)


if __name__ == "__main__":
    main()
