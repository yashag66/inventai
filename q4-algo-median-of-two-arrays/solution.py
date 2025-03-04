
def merge_sorted_arrays(arr1, arr2):
    """
    Merge two sorted arrays
    """
    merged = []
    i, j = 0, 0

    while i < len(arr1) and j < len(arr2):
        if arr1[i] <= arr2[j]:
            merged.append(arr1[i])
            i += 1
        else:
            merged.append(arr2[j])
            j += 1

    # Append remaining elements
    while i < len(arr1):
        merged.append(arr1[i])
        i += 1

    while j < len(arr2):
        merged.append(arr2[j])
        j += 1

    return merged


def find_median(sorted_array):
    """Find the median of a sorted array."""
    n = len(sorted_array)
    mid = n // 2

    if n % 2 == 1:
        return sorted_array[mid]
    else:
        return (sorted_array[mid - 1] + sorted_array[mid]) / 2


if __name__ == "__main__":
    input1 = input("Sorted list input1: ").strip()
    input2 = input("Sorted list input2: ").strip()

    # Converting input string to lists of numbers
    arr1 = list(map(float, input1.split()))
    arr2 = list(map(float, input2.split()))

    merged_array = merge_sorted_arrays(arr1, arr2)
    median_value = find_median(merged_array)

    print(f"output: {median_value}")
