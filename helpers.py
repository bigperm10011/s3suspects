from s3suspects.settings import get_con_string
from sqlalchemy import create_engine, Column, Integer, String, DateTime, MetaData, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import mapper, sessionmaker
from sqlalchemy.ext.automap import automap_base
import sendgrid
import os
from sendgrid.helpers.mail import *
from fuzzywuzzy import fuzz
from geotext import GeoText
import re
import html


#loads db tables from postgres
def load_tables():
    """"""
    cs = get_con_string()
    # automap base
    Base = automap_base()

    # pre-declare User for the 'user' table
    class Leaver(Base):
        __tablename__ = 'leaver'

    # reflect
    engine = create_engine(cs)
    Base.prepare(engine, reflect=True)
    print(engine)
    Suspect = Base.classes.suspect
    Session = sessionmaker(bind=engine)
    session = Session()
    return session, Suspect, Leaver

# to remove html tags from text selected in spider
def remove_html_markup(s):
    tag = False
    quote = False
    out = ""

    for c in s:
            if c == '<' and not quote:
                tag = True
            elif c == '>' and not quote:
                tag = False
            elif (c == '"' or c == "'") and tag:
                quote = not quote
            elif not tag:
                out = out + c

    return out

def list2string(strung):
    print('****** list2string started*****')
    help_list = []
    for s in strung:
        s = str(s)
        s = re.sub(r'[^a-zA-Z0-9\s]', '', s)
        s = s.strip().strip('[').strip(']')
        s = s.replace('\xa0', '')
        s = s.replace('\n', '')
        s = s.lower()
        help_list.append(s)
    lststring = ", ".join(help_list)
    out = re.sub(r'[^a-zA-Z0-9\s]', '', lststring)
    print('final version of string: ', out)

    print('****** list2string closing*****')
    return out

def clean_string(string, name):
    print('******Clean String Started*******')
    first = name.split(' ')[0].lower()
    name = name.lower()
    print('state of string before filtering: ', string)
    view_stmt = 'view ' + name + 's full profile'
    view1_stmt = 'view ' + name + 's profile'
    view2_stmt = 'view ' + name + 's professional profile on linkedin'
    generic_stmt = 'on linkedin the worlds largest professional community'
    dif_stmt = 'find a different ' + name
    join_stmt = 'join linkedin to see ' + first + 's skills endorsements and full profile'
    find_stmt = 'find a different ' + name
    complete_stmt = 'see the complete profile on linkedin and discover ' + first +  's connections and jobs at similar companies'
    partial_stmt = 'see the complete profile on'
    lang_stmt = 'view this profile in another language'
    lang2_stmt = 'view this profile in another  language'
    slang_stmt = 'languages'
    lnkd_stmt = 'linkedin is the worlds largest business network'
    dsc_stmt = 'discover inside'
    help_stmt = 'helping professionals like ' + name
    conn_stmt = 'connections to recommended job candidates'
    trash_list = [lang2_stmt, conn_stmt, view_stmt, view1_stmt, view2_stmt, lnkd_stmt, dif_stmt, help_stmt, dsc_stmt, join_stmt, find_stmt, complete_stmt, partial_stmt, generic_stmt, lang_stmt, slang_stmt]




def zone2(raw_lnk):
    print('*** Starting Zone2 Analysis...')
    print('Content As Delivered: ', raw_lnk)
    clean_link = remove_html_markup(raw_lnk).strip('[').strip(']').strip("\'")
    clean_link = clean_link.strip()
    print('Zone2 Cleaned Link: ', clean_link)
    print('*** Zone2 Analysis Complete ***')
    return clean_link


def zone1(h3):
    print('Content as delivered: ', h3)
    lnkd1 = ' | LinkedIn'
    lnkd2 = ' - LinkedIn'
    lnkd3 = ' | Professional Profile - LinkedIn'
    print('**** Processing Zone 1...')

    regex = re.sub('<[^>]*>', '', str(h3))
    print('regex Phase 1: ', regex)

    regex = regex.strip('[').strip(']').strip("'")
    print('regex Phase 2: ', regex)

    #regex = regex.strip(lnkd1).strip(lnkd2).strip(lnkd3)
    if lnkd1 in regex:
        print(lnkd1)
        regex = regex.strip(lnkd1)
    elif lnkd2 in regex:
        print(lnkd2)
        regex = regex.strip(lnkd2)
    elif lnkd3 in regex:
        print(lnkd3)
        regex = regex.strip(lnkd3)
    print('regex Phase 3: ', regex)

    regex = html.unescape(regex)
    print('regex Phase 4: ', regex)

    regex = regex.strip('.')
    print('regex Phase 5: ', regex)

    dsh_list = regex.split('-')
    print('dash list: ', dsh_list)

    if len(dsh_list) == 3:
        name = dsh_list[0].strip()
        role = dsh_list[1].strip()
        firm = dsh_list[2].strip()
        print('Zone1 Results: Name/Role/Firm Found')
        print(name)
        print(role)
        print(firm)
        print('*** Zone1 Analysis Complete ***')
        return name, role, firm
    elif len(dsh_list) == 4:
        name = dsh_list[0].strip()
        role = dsh_list[1] + ' ' + dsh_list[2].strip()
        firm = dsh_list[3].strip()
        print('Zone1 Results: Name/Role+Role/Firm Found')
        print(name)
        print(role)
        print(firm)
        print('*** Zone1 Analysis Complete ***')
        return name, role, firm
    elif len(dsh_list) == 1:
        name = dsh_list[0].strip()
        role = None
        firm = None
        print('Zone1 Results: name found')
        print(name)
        print('*** Zone1 Analysis Complete ***')
        return name, role, firm
    elif len(dsh_list) == 2:
        name = dsh_list[0].strip()
        role = dsh_list[1].strip()
        firm = None
        print('Zone1 Results: name/role found')
        print(name)
        print(role)
        print('*** Zone1 Analysis Complete ***')
        return name, role, firm
    else:
        name = None
        role = None
        firm = None
        print('Zone1 Results: Nothing Found')
        print('*** Zone1 Analysis Complete ***')
        return name, role, firm

def zone3a(slp):
    print('*** Starting Zone3 Analysis ***')
    print('Content as Delivered: ', slp)
    slp = slp.strip('[').strip(']').strip("'")
    print('stripped brackets: ', slp)
    slp = slp.replace(u'\\xa0', u' ')
    print('Replaced \\xa0: ', slp)
    slp_lst = slp.split('-')
    print('Split Content into List: ', slp_lst)

def score_name(rez_name, db_name):
    print('***Starting Score_Name***')
    ratio = fuzz.ratio(rez_name.lower(), db_name.lower())
    print('Names:')
    print(rez_name, db_name)
    print('ratio is: ', ratio)
    return ratio

def gen_html(added):
    html = """\
        <!DOCTYPE html><html lang="en"><head>SAR Suspects Added Today</head><body><table border='1'>
        <thead><tr><th>Name</th><th>Firm</th><th>Role</th><th>Location</th><th>Link</th></tr></thead>"""

    for l in added:
        html = html + "<tr>"
        html = html + "<td>" + l.name + "</td>"
        try:
            html = html + "<td>" + l.firm + "</td>"
        except:
            html = html + "<td>None</td>"
        try:
            html = html + "<td>" + l.role + "</td>"
        except:
            html = html + "<td>None</td>"
        try:
            html = html + "<td>" + l.location + "</td>"
        except:
            html = html + "<td>None</td>"
        try:
            html = html + '<td><a target="_blank" href="'+ l.link + ' ">LinkedIn</a></td></tr>'
        except:
            html = html + '<td><a target="_blank" href=None">None</a></td></tr>'
    html = html + "</table></body></html>"
    return html

def send_mail(body):
    sg = sendgrid.SendGridAPIClient(apikey=os.environ.get('SENDGRID_API_KEY'))
    from_email = Email("jdbuhrman@gmail.com")
    subject = "SAR Suspects Added Today"
    to_email = Email("jbuhrman2@bloomberg.net")
    content = Content("text/html", body)
    mail = Mail(from_email, subject, to_email, content)
    response = sg.client.mail.send.post(request_body=mail.get())
    return response.status_code
