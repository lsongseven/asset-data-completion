package com.envisioniot.enos.asset.data.completion;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * @Author liang.song lsongseven@gmail.com
 * @Date 2020/11/13 11:39
 */

public class ExternalResourceEvent implements Serializable {

    private static final long serialVersionUID = -2978211497607158141L;

    /**
     * Operation type: create, update, delete
     */
    public String operationType;

    /**
     * resource type
     */
    public String resourceType;

    /**
     * organization id
     */
    public String organizationId;

    /**
     * parent external id
     */
    public String parentExternalId;

    /**
     * external id
     */
    public String externalId;

    /**
     * display name
     * key: default/en_US/zh_CN
     * value: the real name
     */
    public Map<String, String> name;

    /**
     * related actions
     */
    public List<String> actions;

    /**
     * display order
     */
    public int displayOrder;

}
