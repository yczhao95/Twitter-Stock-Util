days = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]

def shift_day(date, shift_d):
    y = int(date[0:4])
    m = int(date[5:7])
    d = int(date[8:10])
    d -= shift_d
    while d < 1:
        m = m - 1;
        if m < 1:
            y -= 1
            m += 12
        d += days[m-1];
        if m == 2 and y % 4 == 0:
            d += 1
   
    
    res = str(y)
    if m < 10 :
        res += "-0" 
    else: 
        res += "-"
    res += str(m)
    if d < 10 : 
        res += "-0" 
    else:  
        res += "-"
    res += str(d)
    return res

def scale_down(result, scale):
    volume = 0.
    cnt = 0
    data = []
    for res in result:
        volume += float(res['volume'])
        cnt += 1
        if cnt % scale == 0 : 
            tmp_entry = {};
            tmp_entry['date'] = res['day']
            tmp_entry['minute'] = res['minute']
            tmp_entry['close'] = res['close']
            tmp_entry['volume'] = volume
            volume= 0
            data.append(tmp_entry)
    return data


if __name__ == '__main__':
    print(shift_day('2018-11-08', 8))
