## Determining if feature is continuous
THRESH = 0.001
def is_cont(data, cat_name):
    print(data[cat_name].nunique() / data[cat_name].count())
    if cat_name == 'human_development_index':
        print("Hello")
    if data[cat_name].dtype != 'float64':
        return False
    if data[cat_name].nunique() / data[cat_name].count() < THRESH:
        return False
    return True
    