lx_count = []
f = 128000
lv = 4/10000*31

for i in range(32):
    lx = round((f-(i*4000))*lv,2)
    lx_count.append(lx)

print(lx_count)
print(sum(lx_count))
