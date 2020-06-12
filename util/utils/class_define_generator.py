def formatter(input= "class_origin", output= "class_define"):
    import re
    attrs_name = []
    attrs_define = []

    with open(input, 'r') as f:
        name = re.match(r".*class\s+(\w+)\s*\(", f.readline()).group(1)
        for l in f.readlines():
            res = re.match(r"\s*(\w+)\s*:.*",l)
            if res:
                attrs_name.append(res.group(1))
                attrs_define.append(l)
    
    with open(output, 'w') as f:
        f.write(f"class {name} ( \n")
        f.writelines( [ l for l in attrs_define ] )
        f.write(""") extends Product() with Serializable{
    def productElement(n: Int) = n match {
""")
        attrs_num = len(attrs_name)
        for i in range(attrs_num):
            f.write(f"{' '*8}case {i} => {attrs_name[i]} \n")
        f.write(f"""    }}
    def canEqual(that: Any) = that.isInstanceOf[{name}]
    def productArity = {attrs_num}
}}""")
    return

if __name__ == "__main__":
    import sys
    if len(sys.argv) == 1:
        formatter()
    elif len(sys.argv) == 2:
        formatter(sys.argv[1])
    elif len(sys.argv) >= 2:
        formatter(sys.argv[1], sys.argv[2])
    else:
        print("Args Error.")




