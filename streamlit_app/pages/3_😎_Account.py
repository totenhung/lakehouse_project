import streamlit as st
from time import sleep
from streamlit_option_menu import option_menu
import yaml
from yaml.loader import SafeLoader
import streamlit_authenticator as stauth

def create_authentication():
    with open('user_management\config.yaml') as file:
        config = yaml.load(file, Loader=SafeLoader)

    authenticator = stauth.Authenticate(
        config['credentials'],
        config['cookie']['name'],
        config['cookie']['key'],
        config['cookie']['expiry_days'],
        config['preauthorized']
    )
    return authenticator

def account_option():
    option = option_menu('', ["Đăng nhập", 'Đăng ký', "Quên mật khẩu", "Đổi mật khẩu"], 
                icons=['box-arrow-in-left', 'pencil-square', 'file-earmark-lock2', 'key'], default_index=0, orientation = "horizontal")
    if option=='Đăng nhập':
        st.session_state.account_option = 'Đăng nhập'
    elif option=='Đăng ký':
        st.session_state.account_option = 'Đăng ký'
    elif option=='Quên mật khẩu':
        st.session_state.account_option = 'Quên mật khẩu'
    elif option=='Đổi mật khẩu':
        st.session_state.account_option = 'Đổi mật khẩu'

def login_page(authenticator):
    name, authentication_status, username = authenticator.login("Đăng nhập", "main")

    if st.session_state["authentication_status"]:
        st.subheader(f'Chào mừng *{st.session_state["name"]}*')
        authenticator.logout('Logout', 'main')

    else :
        if st.session_state["authentication_status"] == False:
            st.error('Username/password is incorrect')
        if st.session_state["authentication_status"] == None:
            st.warning('Đăng kí nếu chưa có tài khoản')
                
def change_pwd(authenticator):
    if st.session_state["authentication_status"]:
        try:
            if authenticator.reset_password(st.session_state["username"], f'Đổi mật khẩu của *{st.session_state["name"]}*'):
                st.success('Đổi mật khẩu thành công')
        except Exception as e:
                st.error(e)
    else:
        st.subheader("Đăng nhập để thực hiện chức năng")


def sign_up_page(authenticator):
    # User create account
    try:
        if authenticator.register_user('Nhập thông tin người dùng', preauthorization=False):
            st.success('Đăng kí tài khoản thành công')
    except Exception as e:
        st.error(e)
    finally:
        st.session_state["sign_up"] = False

def  forgot_pwd_page(authenticator):
    try:
        username_of_forgotten_password, email_of_forgotten_password, new_random_password = authenticator.forgot_password('Nhận mật khẩu mới qua email')
        if username_of_forgotten_password:
            st.success(f'Mật khẩu mới đã được gửi đến *{email_of_forgotten_password}*')
            # Random password should be transferred to user securely
        else:
            st.write('Vui lòng nhập chính xác tên người dùng')

    except Exception as e:
        st.error(e)

authenticator = create_authentication()
account_option()     
if st.session_state.account_option == 'Đăng nhập':
    login_page(authenticator)
if st.session_state.account_option == 'Đăng ký':
    sign_up_page(authenticator)
if st.session_state.account_option == 'Quên mật khẩu':
    forgot_pwd_page(authenticator)
if st.session_state.account_option == 'Đổi mật khẩu':
    change_pwd(authenticator)




