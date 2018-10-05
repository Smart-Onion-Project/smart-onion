import homoglyphs as hg

#print(hg.Languages.detect("א"))
#print(hg.Languages.detect("w"))

str1 = "www.google.com"
str2 = "www.gᴑᴑgle.com"

homoglyphs_cache = {}
homoglyphs_obj = hg.Homoglyphs()
homoglyphs_found = 0
for i in range(0, len(str1)):
    s1 = str1[i]
    s2 = str2[i]
    if not s1 in homoglyphs_cache:
        homoglyphs_cache[s1] = homoglyphs_obj.get_combinations(s1)
    if s2 != s1 and s2 in homoglyphs_cache[s1]:
        print(s2 + " -> " + s1)
        homoglyphs_found = homoglyphs_found + 1

print(str(homoglyphs_found) + " (" + str(homoglyphs_found / len(str1) * 100) + "%)")