package sort;

/**
 * 数组排序
 *
 * @author 　duanjiatao@xgd.com
 * @date 　  2021-01-22 11:13:23
 */
public interface IArraySort {

    /**
     * 数组排序
     *
     * @param sourceArray 待排序数组
     * @return 排序后的数组
     * @throws Exception 异常
     */
    int[] sort(int[] sourceArray) throws Exception;
}
