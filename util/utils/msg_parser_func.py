def parser(msg:str) -> (str, float):
    """
    Args:
        msg: 短信内容
    Returns:
        银行名称:str
        金额:float
    """
    import re
    if not any( [op in msg for op in ["支付","支出","支取","扣款"]] ):
        # 非支付后余额变动短信
        return "", -1
    
    # 获得银行名称
    res1 = re.findall(r"[【\[](\w+行)[\]】]", msg)
    if res1 :
        bank = res1[0]
    elif "工行" in msg:
        bank = "工商银行"
    else:
        # 非法
        return "", -1
    bank = "农业银行" if "农行" in bank else bank

    # 获得金额
    res2 = re.findall(r"((?:\d\,?)+\.\d{2})", msg)
    if not res2:
        return "", -1
    amount = res2[0]

    return bank, amount