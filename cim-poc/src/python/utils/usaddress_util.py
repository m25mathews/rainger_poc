import usaddress


def get_number_street(address:str):
    tagged_address, address_type = usaddress.tag(address, tag_mapping={
        'Recipient': 'recipient',
        'AddressNumber': 'number',
        'AddressNumberPrefix': 'number',
        'AddressNumberSuffix': 'number',
        'StreetName': 'address1',
        'StreetNamePreDirectional': 'address1',
        'StreetNamePreModifier': 'address1',
        'StreetNamePreType': 'address1',
        'StreetNamePostDirectional': 'address1',
        'StreetNamePostModifier': 'address1',
        'StreetNamePostType': 'address1',
        'CornerOf': 'address1',
        'IntersectionSeparator': 'address1',
        'LandmarkName': 'address1',
        'USPSBoxGroupID': 'address1',
        'USPSBoxGroupType': 'address1',
        'USPSBoxID': 'address1',
        'USPSBoxType': 'address1',
        'BuildingName': 'address2',
        'OccupancyType': 'address2',
        'OccupancyIdentifier': 'address2',
        'SubaddressIdentifier': 'address2',
        'SubaddressType': 'address2',
        'PlaceName': 'city',
        'StateName': 'state',
        'ZipCode': 'zip_code',
    })

    return tagged_address['number'], tagged_address['address1']


if __name__ == "__main__":
    address = "Robie House, 5757 South Woodlawn Avenue, Chicago, IL 60637"
    number, street = get_number_street(address)
    print(number)
    print(street)
    # (OrderedDict([
    #     ('address2', u'Robie House'),
    #     ('address1', u'5757 South Woodlawn Avenue'),
    #     ('city', u'Chicago'),
    #     ('state', u'IL'),
    #     ('zip_code', u'60637')]
    # ),
    #  'Street Address')