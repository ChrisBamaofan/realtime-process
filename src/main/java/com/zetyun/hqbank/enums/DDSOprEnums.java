package com.zetyun.hqbank.enums;


/**
 * @author zhaohaojie
 * @date 2024-02-06 9:54
 */
public enum DDSOprEnums {

    UPDATE("u","更新"),
    INSERT("c","插入"),
    DELETE("d","删除");

    String operateName;
    String operateDescribe;

    DDSOprEnums(String operateName,String operateDescribe){
        this.operateDescribe = operateDescribe;
        this.operateName = operateName;
    }

    public String getOperateName() {
        return operateName;
    }

    public void setOperateName(String operateName) {
        this.operateName = operateName;
    }

    public String getOperateDescribe() {
        return operateDescribe;
    }

    public void setOperateDescribe(String operateDescribe) {
        this.operateDescribe = operateDescribe;
    }
}
